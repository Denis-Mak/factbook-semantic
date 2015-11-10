package it.factbook.semantic;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.CassandraConnector$;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import it.factbook.dictionary.Golem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Spark job that runs server for processing requests and performing searches.
 * Before running server it reads all content of idioms_v2 table into memory.
 */
public class SemanticSearch {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    static {
        jsonMapper.registerModule(new JodaModule());
    }
    private static final Logger log = LoggerFactory.getLogger(SemanticSearch.class);

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
        conf.set("spark.kryoserializer.buffer.max", "512M");
        conf.set("spark.cassandra.input.split.size_in_mb", "512");
        conf.set("spark.cassandra.connection.keep_alive_ms", "2000");
        //conf.set("spark.kryo.registrationRequired", "true");
        conf.registerKryoClasses(new Class[]{SemanticKey.class, SemanticSearchWorker.SemanticSearchMatch.class,
                SemanticVector.class, SemanticSearchWorker.SemanticKeyWeightComparator.class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Use here a list only because it is not possible to use arrays with generics,
        // and using list for vectors collection a little bit faster than a map
        List<JavaRDD<SemanticVector>> allVectors = new ArrayList<>(Golem.values().length);
        for (int i = 0; i < Golem.values().length; i++){
            allVectors.add(null);
        }
        for (int golemId: Golem.getValidKeys()) {
            allVectors.set(golemId, javaFunctions(sc)
                    .cassandraTable("doccache", "idiom_v2")
                    .select("golem", "random_index", "mem")
                    .map(row -> new Tuple2<>(row.getInt(0), new SemanticVector(row.getString(1), row.getString(2))))
                    .filter(t -> t._1() == golemId)
                    .map(Tuple2::_2)
                    .persist(StorageLevel.MEMORY_ONLY_SER()));
        }
        CassandraConnector cassandraConnector = CassandraConnector$.MODULE$.apply(sc.getConf());
        SemanticSearchServer semanticSearchServer = new SemanticSearchServer(_semanticSearchPort, allVectors, cassandraConnector, sc);
        semanticSearchServer.start();
        // For the regular (not streaming) jobs shutdown hook doesn't work
        // leave it here before move this app to Spark Streaming
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(semanticSearchServer, log));
    }

    static class ShutdownHook extends Thread {
        SemanticSearchServer semanticSearchServer;
        Logger log;

        ShutdownHook(SemanticSearchServer semanticSearchServer, Logger log){
            this.semanticSearchServer = semanticSearchServer;
            this.log = log;
        }

        @Override
        public void run() {
            System.out.println("Shutdown SemanticSearchServer");
            semanticSearchServer.stop();
        }
    }
}
