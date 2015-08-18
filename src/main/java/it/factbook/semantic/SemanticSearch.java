package it.factbook.semantic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import it.factbook.dictionary.Stem;
import it.factbook.search.SearchProfile;
import it.factbook.search.repository.FactKey;
import it.factbook.search.repository.Match;
import it.factbook.util.BitUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
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
        _keySpace = config.getProperty("cassandra.keyspace");
        //conf.set("spark.kryo.registrationRequired", "true");
        conf.registerKryoClasses(new Class[]{IdiomKey.class, SemanticSearchMatch.class, SemanticVector.class,
                SemanticSearchKeyComparator.class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<SemanticVector> allVectors = javaFunctions(sc)
                .cassandraTable(_keySpace, "idiom_v2")
                .select("golem", "random_index", "mem")
                .map(row -> new SemanticVector(
                        row.getInt(0),
                        BitUtils.reverseHash(row.getString(1), Stem.RI_VECTOR_LENGTH),
                        jsonMapper.readValue(row.getString(2), new TypeReference<int[]>() {
                        })))
                .cache();
        long count = allVectors.count();
        log.debug("All vectors count {}", count);

        final int golemId = 1;
        try (ServerSocket serverSocket = new ServerSocket(_semanticSearchPort)) {
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     PrintWriter out =
                             new PrintWriter(clientSocket.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(clientSocket.getInputStream()));) {
                    String inputLine = in.readLine();
                    if (inputLine != null) {
                        try {
                            SearchProfile profile = jsonMapper.readValue(inputLine, SearchProfile.class);
                            List<IdiomKey> searchKeys = profile.getLines().stream()
                                    .map(line -> new IdiomKey(line.getGolem().getId(), line.getRandomIndex(), line.getMem(), line.getWeight(), 0))
                                    .collect(Collectors.toList());
                            List<Match> matches = search(sc, allVectors, golemId, searchKeys);
                            out.println(jsonMapper.writeValueAsString(matches));
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

    private static List<Match> search(JavaSparkContext sc, JavaRDD<SemanticVector> allVectors, int golemId, List<IdiomKey> searchKeys){
        JavaRDD<IdiomKey> foundVectorsRDD = allVectors
                .filter(vector -> vector.getGolem() == golemId)
                .flatMap(vector -> {
                    List<IdiomKey> foundVectors = new ArrayList<>();
                    for (IdiomKey searchKey : searchKeys) {
                        int commonMems = 0;
                        if (BitUtils.getHammingDistance(vector.getBoolVector(), searchKey.boolVector) < MAX_HAMMING_DIST &&
                                (commonMems = commonMems(searchKey.mem, vector.getMem())) > MIN_COMMON_MEMS) {
                            foundVectors.add(new IdiomKey(
                                    vector.getGolem(),
                                    vector.getBoolVector(),
                                    vector.getMem(),
                                    searchKey.weight,
                                    commonMems
                            ));
                        }
                    }
                    return foundVectors;
                });

        List<IdiomKey> collapsedByRandomIndex = foundVectorsRDD
                .mapToPair(vector -> new Tuple2<>(BitUtils.sparseVectorHash(vector.boolVector, false), vector))
                .reduceByKey((a, b) -> new IdiomKey(a.golem, a.getBoolVector(), a.mem, a.weight + b.weight, a.commonMems))
                .map(Tuple2::_2)
                .top(1000, new SemanticSearchKeyComparator())
                .stream()
                .map(key -> {
                    IdiomKey keyWithHash = new IdiomKey(key);
                    keyWithHash.setRandomIndex(BitUtils.sparseVectorHash(key.boolVector, false));
                    return keyWithHash;
                })
                .collect(Collectors.toList());

        JavaPairRDD<IdiomKey,SemanticSearchMatch> matchesRDD = javaFunctions(sc.parallelize(collapsedByRandomIndex))
                .joinWithCassandraTable(_keySpace, "semantic_index_v2",
                        someColumns("golem", "url", "pos", "factuality", "fingerprint"),
                        someColumns("golem", "random_index"),
                        mapRowTo(SemanticSearchMatch.class),
                        mapToRow(IdiomKey.class));

        JavaPairRDD<FactKey, Match> groupedByFactKey = matchesRDD
                .map(m -> {
                    int factuality = m._2().factuality;
                    int relevancy = new Long(Math.round(Math.log(1 + factuality) * m._1().weight * m._1().commonMems)).intValue();
                    Match match = new Match(0, relevancy);
                    match.attrs.put("url", m._2().url);
                    match.attrs.put("pos", m._2().pos);
                    match.attrs.put("golem", m._2().golem);
                    match.attrs.put("factuality", factuality);
                    match.attrs.put("fingerprint", m._2().fingerprint);
                    return match;
                })
                .keyBy(m -> new FactKey((String)m.attrs.get("url"), (int)m.attrs.get("pos")))
                .reduceByKey((m1, m2) -> {
                    m1.relevance = m1.relevance + m2.relevance;
                    return m1;
                });

        return groupedByFactKey.map(Tuple2::_2).collect();
    }

    public static class SemanticSearchKeyComparator implements Comparator<IdiomKey>, Serializable {
        @Override
        public int compare(IdiomKey a, IdiomKey b) {
            return (a.weight < b.weight) ? -1 : ((a.weight == b.weight) ? 0 : 1);
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
    }

    public static class IdiomKey implements Serializable{
        private int golem;
        private boolean[] boolVector;
        private String randomIndex;
        private int[] mem;
        private int weight;
        private int commonMems;

        public IdiomKey(int golem, boolean[] boolVector, int[] mem, int weight, int commonMems) {
            this.golem = golem;
            this.boolVector = boolVector;
            this.mem = mem;
            this.weight = weight;
            this.commonMems = commonMems;
        }

        public IdiomKey(IdiomKey src){
            this.golem = src.golem;
            this.boolVector = src.boolVector;
            this.mem = src.mem;
            this.weight = src.weight;
            this.commonMems = src.commonMems;
        }

        public int getGolem() {
            return golem;
        }

        public void setGolem(int golem) {
            this.golem = golem;
        }

        public boolean[] getBoolVector() {
            return boolVector;
        }

        public void setBoolVector(boolean[] boolVector) {
            this.boolVector = boolVector;
        }

        public int[] getMem() {
            return mem;
        }

        public void setMem(int[] mem) {
            this.mem = mem;
        }

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

        public int getCommonMems() {
            return commonMems;
        }

        public void setCommonMems(int commonMems) {
            this.commonMems = commonMems;
        }

        public String getRandomIndex() {
            return randomIndex;
        }

        public void setRandomIndex(String randomIndex) {
            this.randomIndex = randomIndex;
        }
    }

}
