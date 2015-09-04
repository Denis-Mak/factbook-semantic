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
        //conf.set("spark.kryo.registrationRequired", "true");
        conf.registerKryoClasses(new Class[]{SemanticKey.class, SemanticSearchWorker.SemanticSearchMatch.class,
                SemanticVector.class, SemanticSearchWorker.SemanticKeyMemsComparator.class});
        JavaSparkContext sc = new JavaSparkContext(conf);
        Map<Integer, JavaRDD<SemanticVector>> allVectors = new HashMap<>(Golem.values().length - 1);
        for (int golemId: Golem.getValidKeys()) {
            allVectors.put(golemId, javaFunctions(sc)
                    .cassandraTable("doccache", "idiom_v2")
                    .select("golem", "random_index", "mem")
                    .map(row -> new Tuple2<>(row.getInt(0), new SemanticVector(row.getString(1), row.getString(2))))
                    .filter(t -> t._1() == golemId)
                    .map(Tuple2::_2)
                    .persist(StorageLevel.MEMORY_ONLY_SER()));
            long count = allVectors.get(golemId).count();
            log.debug("All vectors for golem: {} -> count {}", golemId, count);
        }

        SemanticSearchServer semanticSearchServer = new SemanticSearchServer(_semanticSearchPort, allVectors);
        semanticSearchServer.start();
        // For the regular (not streaming) jobs shutdown hook doen't work
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
