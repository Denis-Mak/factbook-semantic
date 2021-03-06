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
import it.factbook.search.SearchProfile;
import it.factbook.search.repository.Idiom;
import it.factbook.search.repository.Match;
import it.factbook.util.BitUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Class that performs semantic search.
 */
class SemanticSearchWorker implements Runnable {
    private static final int MAX_HAMMING_DIST = 16;
    private static final int MIN_COMMON_MEMS = 4;
    private static final long MAX_RESULTS_PER_RDD = 2000L;

    private static final Logger log = LoggerFactory.getLogger(SemanticSearchWorker.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    static {
        jsonMapper.registerModule(new JodaModule());
    }

    private Socket clientSocket;

    private List<JavaRDD<SemanticVector>> allVectors;

    private CassandraConnector cassandraConnector;

    private JavaSparkContext sparkContext;

    /**
     * Creates an instance of SemanticSearchWorker to run in a thread
     *
     * @param clientSocket socket where to return results
     * @param allVectors collection of all possible idioms
     * @param cassandraConnector an instance of {@link CassandraConnector} to read from database
     * @param sc spark context to perform operation in Spark
     */
    public SemanticSearchWorker(Socket clientSocket, List<JavaRDD<SemanticVector>> allVectors,
                                CassandraConnector cassandraConnector, JavaSparkContext sc) {
        this.clientSocket = clientSocket;
        this.allVectors = allVectors;
        this.cassandraConnector = cassandraConnector;
        this.sparkContext = sc;
    }

    /**
     * Main entry point reads and parses a JSON message from socket. If there is no errors with the message it runs
     * semantic search tasks. In case of errors it writes messages to the log and closes socket.
     */
    @Override
    public void run() {
        log.debug("Get request");
        BufferedReader in = null;
        PrintWriter out = null;
        try {
            in = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
            out = new PrintWriter(this.clientSocket.getOutputStream(), true);
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
                        if (searchKeys.isEmpty()) {
                            return;
                        }
                        searchKeys.stream()
                                .forEach(e ->
                                        log.debug("Line to search, random vector: {}, mem: {}",
                                                e.getRandomIndex(),
                                                e.getMem()));
                        if (action == 1) {
                            List<Match> matches = search(allVectors.get(golemId), searchKeys, cassandraConnector);
                            out.println(jsonMapper.writeValueAsString(matches));
                        } else {
                            List<Idiom> idioms = getSimilarIdioms(allVectors.get(golemId), searchKeys, cassandraConnector);
                            out.println(jsonMapper.writeValueAsString(idioms));
                        }
                    } else {
                        log.error("Profile must have at least one line with valid golem.");
                    }
                } catch (IOException e) {
                    log.error("Error parse SearchProfile.class JSON", e);
                }
            }
        } catch (IOException e) {
            log.error("Error open client socket to read/write", e);
        } finally {
            try {
                if (out != null) out.close();
                if (in != null) in.close();
            } catch (IOException e) {
                log.error("Error close client socket", e);
            }
        }
    }

    /**
     * Searches facts that contains semantically close idioms to the ones from the profile.
     * All found synonyms are rated by proximity to the queried and content-richness.
     *
     * @param allVectors collection of all possible idioms
     * @param searchKeys idioms from the profile packed into {@link SemanticKey} containers
     * @param cassandraConnector an instance of {@link CassandraConnector} to read from database
     * @return list of found matches
     */
    private List<Match> search(JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys,
                                      CassandraConnector cassandraConnector) {
        JavaPairRDD<SemanticKey, SemanticSearchMatch> matchesRDD =
                customJavaFunctions(getSimilarIdiomKeys(allVectors, searchKeys, cassandraConnector))
                .joinWithCassandraTable("doccache", "semantic_index_v2",
                        someColumns("golem", "url", "pos", "factuality", "fingerprint", "idiom"),
                        someColumns("golem", "random_index", "mem"),
                        mapRowTo(SemanticSearchMatch.class),
                        mapToRow(SemanticKey.class));
        List<Tuple2<SemanticKey, SemanticSearchMatch>> matchesList = matchesRDD.collect();
        log.debug("Found {} matches", matchesList.size());
        return matchesList.stream()
                .map(m -> {
                    int factuality = m._2().factuality;
                    int relevancy = new Long(Math.round(Math.log(1 + factuality) * m._1().getWeight())).intValue();
                    Match match = new Match(0, relevancy);
                    match.attrs.put("url", m._2().url);
                    match.attrs.put("pos", m._2().pos);
                    match.attrs.put("golem", m._2().golem);
                    match.attrs.put("factuality", factuality);
                    match.attrs.put("fingerprint", m._2().fingerprint);
                    match.attrs.put("idiom", m._2().idiom);
                    return match;
                })
                .collect(Collectors.toList());
    }

    /**
     * Searches synonyms of the provided idioms.
     *
     * @param allVectors collection of all possible idioms
     * @param searchKeys idioms from the profile packed into {@link SemanticKey} containers
     * @param cassandraConnector an instance of {@link CassandraConnector} to read from database
     * @return list of found idioms ordered by count of common mems
     */
    private static List<Idiom> getSimilarIdioms(JavaRDD<SemanticVector> allVectors, List<SemanticKey> searchKeys,
                                                CassandraConnector cassandraConnector) {
        JavaPairRDD<SemanticKey, Idiom> idioms = javaFunctions(getSimilarIdiomKeys(allVectors, searchKeys, cassandraConnector))
                .joinWithCassandraTable("doccache", "idiom_v2",
                        someColumns("golem", "random_index", "mem", "idiom"),
                        someColumns("golem", "random_index", "mem"),
                        mapRowTo(Idiom.class),
                        mapToRow(SemanticKey.class));
        SemanticKeyWeightComparator comparator = new SemanticKeyWeightComparator();
        return idioms.collect().stream()
                .sorted((p1, p2) -> comparator.compare(p1._1(), p2._1()))
                .map(pair -> {
                    Idiom idiom = pair._2();
                    idiom.setWeight(pair._1().getWeight());
                    return idiom;
                })
                .collect(Collectors.toList());
    }

    /**
     * Find close by sense idioms comparing random vectors and mems
     *
     * @param allVectors collection of all possible idioms known by the system
     * @param searchKeys collection of idioms from the profile using as keys for searching
     * @param cassandraConnector an instance of {@link CassandraConnector} to read/write from database
     * @return collection of found keys (golem + random_vector + mem) close enough to the queried
     */
    private static JavaRDD<SemanticKey> getSimilarIdiomKeys(final JavaRDD<SemanticVector> allVectors,
                                                            final List<SemanticKey> searchKeys,
                                                            CassandraConnector cassandraConnector) {
        return allVectors
                .map(vector -> {
                    int foundInLine = 0;
                    SemanticKey foundKey = null;
                    for (SemanticKey searchKey : searchKeys) {
                        int commonMems;
                        if (checkDistance(vector.getBoolVector(), searchKey.getBoolVectorAsLongs()) &&
                                (commonMems = commonMems(vector.getMem(), searchKey.getMemIntArr(), MIN_COMMON_MEMS)) > MIN_COMMON_MEMS) {
                            if (foundKey == null) {
                                foundKey = new SemanticKey(
                                        searchKey.getGolem(),
                                        BitUtils.convertToBits(vector.getBoolVector()),
                                        vector.getMem(),
                                        searchKey.getWeight() * commonMems,
                                        foundInLine);
                            }else {
                                foundKey.setWeight(foundKey.getWeight() + searchKey.getWeight());
                            }
                        }
                        foundInLine++;
                    }
                    return foundKey;
                })
                .filter(vector -> vector != null)
                .mapToPair(key -> new Tuple2<>(key.getFoundInLine(), key))
                .aggregateByKey(
                        new ArrayList<SemanticKey>(),
                        40,
                        (list, elm) -> {
                            list.add(elm);
                            return list;
                        },
                        (list1, list2) -> {
                            list1.addAll(list2);
                            return list1;
                        })
                .flatMap(tuple ->
                        tuple._2().stream()
                                .sorted(new SemanticKeyWeightComparator())
                                .limit(50)
                                .collect(Collectors.toList()));
    }

    public static class SemanticKeyWeightComparator implements Comparator<SemanticKey>, Serializable {
        @Override
        public int compare(SemanticKey a, SemanticKey b) {
            // for DESC ordering comparator inverted
            return (b.getWeight() < a.getWeight()) ? -1 : ((a.getWeight() == b.getWeight()) ? 0 : 1);
        }
    }

    /**
     * Checks if hamming distance between two vectors is close enough to consider them as synonyms
     *
     * @param vec1 vector one
     * @param vec2 vector two
     * @return true if distance between the vectors is lower then {@link #MAX_HAMMING_DIST}
     */
    private static boolean checkDistance(long[] vec1, long[] vec2) {
        return BitUtils.getHammingDistance(vec1, vec2, 0, 1, MAX_HAMMING_DIST) < MAX_HAMMING_DIST &&
                BitUtils.getHammingDistance(vec1, vec2, 1, vec2.length, MAX_HAMMING_DIST) < MAX_HAMMING_DIST;
    }

    /**
     * Count same memIds in two vectors
     *
     * @param patternMem vector one
     * @param memVariant vector two
     * @param minCommonMems minimum count if it is not possible to reach it calculation stops
     * @return count of the same memIds
     */
    static int commonMems(int[] patternMem, int[] memVariant, int minCommonMems) {
        int commonMems = 0;
        for (int i = 0, n = patternMem.length; i < n; i++) {
            // If we see that cannot collect more than minimum common mems - return earlier
            int memLeft = n - i;
            if (memLeft < minCommonMems && minCommonMems - commonMems > memLeft) {
                return commonMems;
            }
            for (int aMemVariant : memVariant) {
                if (patternMem[i] == aMemVariant) {
                    commonMems++;
                    break;
                }
            }
        }
        return commonMems;
    }

    /**
     * Container of temporary matches, this bean is needed to perform mapping in the Cassandra-Spark connector
     */
    public static class SemanticSearchMatch implements Serializable {
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

    /**
     * This class is overridden to specify limit in join, otherwise task crashes on the very "wide" synonyms
     * @param <T>
     */
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
            Option<Object> limit = Option.apply(MAX_RESULTS_PER_RDD);
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
