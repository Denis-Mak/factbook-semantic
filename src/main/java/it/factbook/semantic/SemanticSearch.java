package it.factbook.semantic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import it.factbook.dictionary.Golem;
import it.factbook.search.SearchProfile;
import it.factbook.search.repository.FactKey;
import it.factbook.search.repository.Idiom;
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
                            final int golemId = profile.getGolems().iterator().next().getId();
                            if (golemId > 0) {
                                if (profile.getGolems().size() > 1) {
                                    log.warn("It is impossible in this version search by multiple golem profiles. " +
                                            "Search using golem {}", golemId);
                                }
                                List<SemanticKey> searchKeys = profile.getLines().stream()
                                        .map(line -> new SemanticKey(golemId, line.getRandomIndex(), line.getMem(), line.getWeight(), 0))
                                        .collect(Collectors.toList());
                                if (action == 1) {
                                    List<Match> matches = search(sc, allVectors.get(golemId), searchKeys);
                                    out.println(jsonMapper.writeValueAsString(matches));
                                } else {
                                    List<Idiom> idioms = getSimilarIdioms(sc, allVectors.get(golemId), searchKeys);
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

    private static List<Match> search(JavaSparkContext sc, JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys){
        List<SemanticKey> collapsedByRandomIndex = getSimilarIdiomKeys(allVectors, searchKeys);

        JavaPairRDD<SemanticKey,SemanticSearchMatch> matchesRDD = javaFunctions(sc.parallelize(collapsedByRandomIndex))
                .joinWithCassandraTable(_keySpace, "semantic_index_v2",
                        someColumns("golem", "url", "pos", "factuality", "fingerprint"),
                        someColumns("golem", "random_index"),
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
                    return match;
                })
                .sorted((m1, m2) -> Integer.compare(m2.relevance, m1.relevance))
                .limit(1000)
                .collect(Collectors.toList());
    }

    private static List<Idiom> getSimilarIdioms(JavaSparkContext sc, JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys){
        List<SemanticKey> keysToIdioms = getSimilarIdiomKeys(allVectors, searchKeys);

        JavaPairRDD<SemanticKey,Idiom> idioms = javaFunctions(sc.parallelize(keysToIdioms))
                .joinWithCassandraTable(_keySpace, "idiom_v2",
                        someColumns("golem","random_index", "mem", "idiom"),
                        someColumns("golem", "random_index"),
                        mapRowTo(Idiom.class),
                        mapToRow(SemanticKey.class));
        return idioms.map(Tuple2::_2).collect();
    }

    private static List<SemanticKey> getSimilarIdiomKeys(JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys){
        JavaRDD<SemanticKey> foundVectorsRDD = allVectors
                .flatMap(vector -> {
                    List<SemanticKey> foundVectors = new ArrayList<>();
                    for (SemanticKey searchKey : searchKeys) {
                        int commonMems = 0;
                        if (BitUtils.getHammingDistance(vector.getBoolVector(), searchKey.getBoolVectorAsLongs(), 0, 1) < MAX_HAMMING_DIST &&
                                BitUtils.getHammingDistance(vector.getBoolVector(), searchKey.getBoolVectorAsLongs(), 1, searchKey.getBoolVectorAsLongs().length) < MAX_HAMMING_DIST &&
                                (commonMems = commonMems(searchKey.getMem(), vector.getMem())) > MIN_COMMON_MEMS) {
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

        return foundVectorsRDD
                .mapToPair(vector -> new Tuple2<>(vector.getRandomIndex(), vector))
                .reduceByKey((a, b) ->
                        new SemanticKey(a.getGolem(), a.getRandomIndex(), a.getMem(), a.getWeight() + b.getWeight(), a.getCommonMems()))
                .map(Tuple2::_2)
                .top(1000, new SemanticSearchKeyComparator());
    }

    public static class SemanticSearchKeyComparator implements Comparator<SemanticKey>, Serializable {
        @Override
        public int compare(SemanticKey a, SemanticKey b) {
            return (a.getWeight() < b.getWeight()) ? -1 : ((a.getWeight() == b.getWeight()) ? 0 : 1);
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

}
