package it.factbook.semantic;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.RDDJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.rdd.CassandraJoinRDD;
import com.datastax.spark.connector.rdd.ClusteringOrder;
import com.datastax.spark.connector.rdd.CqlWhereClause;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import it.factbook.dictionary.Golem;
import it.factbook.search.SearchProfile;
import it.factbook.search.repository.Idiom;
import it.factbook.search.repository.Match;
import it.factbook.util.BitUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SemanticSearch {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    static {
        jsonMapper.registerModule(new JodaModule());
    }
    private static final Logger log = LoggerFactory.getLogger(SemanticSearch.class);
    private static final int MAX_HAMMING_DIST = 34;
    private static final int MIN_COMMON_MEMS = 4;
    private static String _keySpace = "doccache";

    public static void main(String[] args) {
        Properties config = Util.readProperties();
        SparkConf conf = new SparkConf();
        conf.setAppName("Semantic Search");
        conf.setMaster(config.getProperty("spark.master"));
        conf.set("spark.executor.memory", config.getProperty("spark.executor.memory"));
        conf.set("spark.cassandra.connection.host", config.getProperty("cassandra.host"));
        conf.set("spark.cassandra.auth.username", config.getProperty("cassandra.user"));
        conf.set("spark.cassandra.auth.password", config.getProperty("cassandra.password"));
        int _semanticSearchPort = Integer.parseInt(config.getProperty("semantic.search.port"));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryoserializer.buffer.max.mb", "512");
        _keySpace = config.getProperty("cassandra.keyspace");
        //conf.set("spark.kryo.registrationRequired", "true");
        conf.registerKryoClasses(new Class[]{SemanticKey.class, SemanticSearchMatch.class, SemanticVector.class,
                SemanticSearchKeyComparator.class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        Map<Integer, JavaRDD<SemanticVector>> allVectors = new HashMap<>(Golem.values().length-1);
        for (int golemId: Golem.getValidKeys()) {
            allVectors.put(golemId, javaFunctions(sc)
                    .cassandraTable(_keySpace, "idiom_v2")
                    .select("golem", "random_index", "mem")
                    .map(row -> new Tuple2<>(row.getInt(0), new SemanticVector(row.getString(1), row.getString(2))))
                    .filter(t -> t._1() == golemId)
                    .map(Tuple2::_2)
                    .persist(StorageLevel.MEMORY_ONLY_SER()));
            long count = allVectors.get(golemId).count();
            log.debug("All vectors for golem: {} -> count {}", golemId, count);
        }

        try (ServerSocket serverSocket = new ServerSocket(_semanticSearchPort)) {
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     PrintWriter out =
                             new PrintWriter(clientSocket.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(clientSocket.getInputStream()))) {
                    String inputLine = in.readLine();
                    if (inputLine != null) {
                        try {
                            JsonNode root = jsonMapper.readTree(inputLine);
                            int action = root.get("actionCode").asInt();
                            SearchProfile profile = jsonMapper.treeToValue(root.get("profile"), SearchProfile.class);
                            final int golemId;
                            if (profile.getGolems().size() > 0 &&
                                    (golemId = profile.getGolems().iterator().next().getId()) > 0) {
                                if (profile.getGolems().size() > 1) {
                                    log.warn("It is impossible in this version search by multiple golem profiles. " +
                                            "Search using golem {}", golemId);
                                }
                                List<SemanticKey> searchKeys = profile.getLines().stream()
                                        .filter(line -> line.getMem().length > 0 &&
                                                line.getRandomIndex().length > 0)
                                        .map(line -> new SemanticKey(golemId, line.getRandomIndex(), line.getMem(), line.getWeight(), 0))
                                        .collect(Collectors.toList());
                                if (searchKeys.isEmpty()){
                                    continue;
                                }
                                searchKeys.stream()
                                        .forEach(e ->
                                                log.debug("Line to search, random vector: {}, mem: {}",
                                                        e.getRandomIndex(),
                                                        e.getMem()));
                                if (action == 1) {
                                    List<Match> matches = search(allVectors.get(golemId), searchKeys);
                                    out.println(jsonMapper.writeValueAsString(matches));
                                } else {
                                    List<Idiom> idioms = getSimilarIdioms(allVectors.get(golemId), searchKeys);
                                    out.println(jsonMapper.writeValueAsString(idioms));
                                }
                            } else {
                                log.error("Profile must have at least one line with valid golem.");
                            }
                        } catch (IOException e) {
                            log.error("Error parse SearchProfile.class JSON: {}", e);
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            log.error("Exception caught when trying to listen on port "
                    + " or listening for a connection \n {}", e);
        }
    }

    private static List<Match> search(JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys){
        List<SemanticKey> keys = getSimilarIdiomKeys(allVectors, searchKeys).collect();
        JavaPairRDD<SemanticKey,SemanticSearchMatch> matchesRDD = customJavaFunctions(getSimilarIdiomKeys(allVectors, searchKeys))
                .joinWithCassandraTable(_keySpace, "semantic_index_v2",
                        someColumns("golem", "url", "pos", "factuality", "fingerprint", "idiom"),
                        someColumns("golem", "random_index", "mem"),
                        mapRowTo(SemanticSearchMatch.class),
                        mapToRow(SemanticKey.class));

        List<Tuple2<SemanticKey, SemanticSearchMatch>> matches = matchesRDD.collect();
        return matches.stream()
                .map(m -> {
                    int factuality = m._2().factuality;
                    int relevancy = new Long(Math.round(Math.log(1 + factuality) * m._1().getWeight() * m._1().getCommonMems())).intValue();
                    Match match = new Match(0, relevancy);
                    match.attrs.put("url", m._2().url);
                    match.attrs.put("pos", m._2().pos);
                    match.attrs.put("golem", m._2().golem);
                    match.attrs.put("factuality", factuality);
                    match.attrs.put("fingerprint", m._2().fingerprint);
                    match.attrs.put("idiom", m._2().idiom);
                    return match;
                })
                .sorted((m1, m2) -> Integer.compare(m2.relevance, m1.relevance))
                .limit(1000)
                .collect(Collectors.toList());
    }

    private static List<Idiom> getSimilarIdioms(JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys){
        JavaPairRDD<SemanticKey,Idiom> idioms = javaFunctions(getSimilarIdiomKeys(allVectors, searchKeys))
                .joinWithCassandraTable(_keySpace, "idiom_v2",
                        someColumns("golem", "random_index", "mem", "idiom"),
                        someColumns("golem", "random_index", "mem"),
                        mapRowTo(Idiom.class),
                        mapToRow(SemanticKey.class));
        return idioms
                .sortByKey(new SemanticSearchKeyComparator())
                .map(pair -> {
                    pair._2().setWeight(pair._1().getCommonMems());
                    return pair._2();
                }).collect();
    }

    private static JavaRDD<SemanticKey> getSimilarIdiomKeys(JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys){
        return allVectors
                .flatMap(vector -> {
                    List<SemanticKey> foundVectors = new ArrayList<>();
                    for (SemanticKey searchKey : searchKeys) {
                        int commonMems = 0;
                        if (BitUtils.getHammingDistance(vector.getBoolVector(), searchKey.getBoolVectorAsLongs(), 0, 1) < MAX_HAMMING_DIST &&
                                BitUtils.getHammingDistance(vector.getBoolVector(), searchKey.getBoolVectorAsLongs(), 1, searchKey.getBoolVectorAsLongs().length) < MAX_HAMMING_DIST &&
                                (commonMems = commonMems(searchKey.getMemIntArr(), vector.getMem())) > MIN_COMMON_MEMS) {
                            foundVectors.add(new SemanticKey(
                                    searchKey.getGolem(),
                                    BitUtils.convertToBits(vector.getBoolVector()),
                                    vector.getMem(),
                                    searchKey.getWeight(),
                                    commonMems
                            ));
                        }
                    }
                    return foundVectors;
                });
    }

    public static class SemanticSearchKeyComparator implements Comparator<SemanticKey>, Serializable {
        @Override
        public int compare(SemanticKey a, SemanticKey b) {
            // for DESC ordering comparator inverted
            return (b.getWeight() < a.getWeight()) ? -1 : ((a.getWeight() == b.getWeight()) ? 0 : 1);
        }
    }

    private static int commonMems (int[] patternMem, int[] memVariant) {
        int commonMems = 0;
        for (int aPatternMem : patternMem) {
            for (int aMemVariant : memVariant) {
                if (aPatternMem == aMemVariant) {
                    commonMems++;
                    break;
                }
            }
        }
        return commonMems;
    }

    public static class SemanticSearchMatch implements Serializable{
        private int golem;
        private String url;
        private int pos;
        private int fingerprint;
        private int factuality;
        private String idiom;

        public SemanticSearchMatch(int golem, String url, int pos) {
            this.golem = golem;
            this.url = url;
            this.pos = pos;
        }

        public int getGolem() {
            return golem;
        }

        public void setGolem(int golem) {
            this.golem = golem;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public int getPos() {
            return pos;
        }

        public void setPos(int pos) {
            this.pos = pos;
        }

        public int getFingerprint() {
            return fingerprint;
        }

        public void setFingerprint(int fingerprint) {
            this.fingerprint = fingerprint;
        }

        public int getFactuality() {
            return factuality;
        }

        public void setFactuality(int factuality) {
            this.factuality = factuality;
        }

        public String getIdiom() {
            return idiom;
        }

        public void setIdiom(String idiom) {
            this.idiom = idiom;
        }
    }

    static class CustomRDDJavaFunctions<T> extends RDDJavaFunctions<T> {
        public CustomRDDJavaFunctions(RDD<T> rdd) {
            super(rdd);
        }

        @Override
        public <R> CassandraJavaPairRDD<T, R> joinWithCassandraTable(
                String keyspaceName,
                String tableName,
                ColumnSelector selectedColumns,
                ColumnSelector joinColumns,
                RowReaderFactory<R> rowReaderFactory,
                RowWriterFactory<T> rowWriterFactory
        ) {
            ClassTag<T> classTagT = rdd.toJavaRDD().classTag();
            ClassTag<R> classTagR = JavaApiHelper.getClassTag(rowReaderFactory.targetClass());

            CassandraConnector connector = defaultConnector();
            Option<ClusteringOrder> clusteringOrder = Option.empty();
            Option<Object> limit = Option.apply(1000L);
            CqlWhereClause whereClause = CqlWhereClause.empty();
            ReadConf readConf = ReadConf.fromSparkConf(rdd.conf());

            CassandraJoinRDD<T, R> joinRDD = new CassandraJoinRDD<>(
                    rdd,
                    keyspaceName,
                    tableName,
                    connector,
                    selectedColumns,
                    joinColumns,
                    whereClause,
                    limit,
                    clusteringOrder,
                    readConf,
                    classTagT,
                    classTagR,
                    rowWriterFactory,
                    rowReaderFactory);

            return new CassandraJavaPairRDD<>(joinRDD, classTagT, classTagR);
        }
    }

    private static <T> RDDJavaFunctions<T> customJavaFunctions(JavaRDD<T> rdd) {
        return new CustomRDDJavaFunctions<>(rdd.rdd());
    }
}
