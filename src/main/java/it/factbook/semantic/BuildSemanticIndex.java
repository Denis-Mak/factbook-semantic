package it.factbook.semantic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import gnu.trove.map.hash.*;
import it.factbook.dictionary.Golem;
import it.factbook.dictionary.LangDetectorCybozuImpl;
import it.factbook.dictionary.WordForm;
import it.factbook.dictionary.repository.StemAdapter;
import it.factbook.dictionary.repository.inmemory.StemAdapterInMemoryImpl;
import it.factbook.dictionary.repository.inmemory.WordFormAdapterInMemoryImpl;
import it.factbook.search.Fact;
import it.factbook.search.FactProcessor;
import it.factbook.search.repository.jdbc.ClassifierAdapterImpl;
import it.factbook.util.BitUtils;
import it.factbook.util.TextSplitterOpenNlpRuImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class BuildSemanticIndex {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(BuildSemanticIndex.class);
    private static final class FactProcessorWrapper{
        private static FactProcessor factProcessor;
        static {
            Properties config = Util.readProperties();
            MysqlDataSource dictionaryDataSource = new MysqlDataSource();
            dictionaryDataSource.setURL(config.getProperty("jdbc.dictionary.url"));
            dictionaryDataSource.setUser(config.getProperty("jdbc.dictionary.username"));
            dictionaryDataSource.setPassword(config.getProperty("jdbc.dictionary.password"));
            MysqlDataSource docCacheDataSource = new MysqlDataSource();
            docCacheDataSource.setURL(config.getProperty("jdbc.doccache.url"));
            docCacheDataSource.setUser(config.getProperty("jdbc.doccache.username"));
            docCacheDataSource.setPassword(config.getProperty("jdbc.doccache.password"));
            factProcessor = FactProcessor.getInstance(docCacheDataSource, dictionaryDataSource);
        }
        public static FactProcessor getFactProcessor(){
            return factProcessor;
        }
    }

    public static void main(String[] args) {
        Properties config = Util.readProperties();

        SparkConf conf = new SparkConf();
        conf.setAppName("Build Semantic Index");
        conf.setMaster(config.getProperty("spark.master"));
        conf.set("spark.executor.memory", config.getProperty("spark.executor.memory"));
        conf.set("spark.driver.memory", config.getProperty("spark.driver.memory"));
        conf.set("spark.cassandra.connection.host", config.getProperty("cassandra.host"));
        conf.set("spark.cassandra.auth.username", config.getProperty("cassandra.user"));
        conf.set("spark.cassandra.auth.password", config.getProperty("cassandra.password"));
        //conf.set("spark.cassandra.input.split.size_in_mb", "10");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "true");
        conf.set("spark.kryoserializer.buffer.max", "512M");
        //conf.set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
        conf.registerKryoClasses(new Class[]{Fact.class, FactProcessor.class,
                ClassifierAdapterImpl.class, SemanticIndexRow.class, WordFormAdapterInMemoryImpl.class,
                StemAdapterInMemoryImpl.class, TextSplitterOpenNlpRuImpl.class, LangDetectorCybozuImpl.class,
                TIntIntHashMap.class, TLongIntHashMap.class, TByteObjectHashMap.class,
                TIntObjectHashMap.class, TLongObjectHashMap.class});
        String _keySpace = config.getProperty("cassandra.keyspace");
        String _factTable = config.getProperty("cassandra.fact.table");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Fact> facts = javaFunctions(sc)
                .cassandraTable(_keySpace, _factTable)
                .select("url", "pos", "golem", "proto", "content", "fingerprint", "factuality")
                .map(row -> {
                    String url = row.getString("proto") + "://" + row.getString("url");
                    int pos = row.getInt("pos");
                    try {
                        return new Fact.Builder()
                                .content(row.getString("content"))
                                .url(url)
                                .factuality(row.getInt("factuality"))
                                .fingerprint(BitUtils.convertToBits(row.getInt("fingerprint")))
                                .golem(Golem.valueOf(row.getInt("golem")))
                                .pos(pos)
                                .build();
                    } catch (Exception e) {
                        log.error("Error read fact URL:{} pos:{} \n {}", url, pos, e);
                    }
                    return new Fact.Builder().build();
                });

        JavaRDD<SemanticIndexRow> idiomsInFacts = facts
                .flatMap(f -> {
                    try {
                        FactProcessor factProcessor = FactProcessorWrapper.getFactProcessor();
                        List<WordForm[]> factIdioms = factProcessor.getIdioms(
                                factProcessor.getTrees(f.getGolem(), f.getContent()));
                        List<int[]> sense = factProcessor.convertToSense(factIdioms);
                        StemAdapter stemAdapter = factProcessor.getStemAdapter();
                        List<SemanticIndexRow> rows = new ArrayList<>(factIdioms.size());
                        for (int i = 0; i < factIdioms.size(); i++) {
                            StringJoiner sj = new StringJoiner(", ");
                            for (WordForm wf : factIdioms.get(i)) {
                                sj.add(wf.getWord());
                            }
                            SemanticIndexRow semanticIndexRow = new SemanticIndexRow();
                            semanticIndexRow.setGolem(f.getGolem().getId()) ;
                            semanticIndexRow.setRandomIndex(BitUtils.sparseVectorHash(stemAdapter.getRandomIndexingVector(f.getGolem(), sense.get(i)), false));
                            semanticIndexRow.setMem(jsonMapper.writeValueAsString(sense.get(i)));
                            semanticIndexRow.setUrl(f.getUrlObj().getHost() + f.getUrlObj().getFile());
                            semanticIndexRow.setPos(f.getPos());
                            semanticIndexRow.setFactuality(f.getFactuality());
                            semanticIndexRow.setFingerprint(f.getFingerprintAsInt());
                            semanticIndexRow.setIdiom(sj.toString());
                            rows.add(semanticIndexRow);
                        }
                        return rows;
                    } catch (Exception e) {
                        log.error("Error during parsing fact URL:{} pos: {}\n Stacktrace {} ", f.getUrl(), f.getPos(), e);
                    }
                    return Collections.<SemanticIndexRow>emptyList();
                });

        javaFunctions(idiomsInFacts)
                .writerBuilder(_keySpace, "semantic_index_v2", mapToRow(SemanticIndexRow.class))
                .saveToCassandra();
    }


}
