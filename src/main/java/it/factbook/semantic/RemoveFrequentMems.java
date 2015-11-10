package it.factbook.semantic;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.CassandraConnector$;
import com.datastax.spark.connector.japi.CassandraRow;
import com.google.common.collect.Iterators;
import it.factbook.dictionary.Golem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Spark job that finds and removes top by frequency in texts idioms, typically names of people or places that
 * are not much efficient for semantic searching.
 */
public class RemoveFrequentMems {
    private static final Logger log = LoggerFactory.getLogger(RemoveFrequentMems.class);

    public static void main(String[] args) {
        Properties config = Util.readProperties();

        SparkConf conf = new SparkConf();
        conf.setAppName("Remove frequent mems");
        conf.setMaster(config.getProperty("spark.master"));
        conf.set("spark.cassandra.connection.host", config.getProperty("cassandra.host"));
        conf.set("spark.cassandra.auth.username", config.getProperty("cassandra.user"));
        conf.set("spark.cassandra.auth.password", config.getProperty("cassandra.password"));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "true");
        conf.set("spark.kryoserializer.buffer.max", "512M");
        conf.set("spark.executor.memory", "7G");
        conf.set("spark.cassandra.connection.keep_alive_ms", "2000");
        conf.set("spark.cassandra.input.split.size_in_mb", "32");
        conf.set("spark.cassandra.read.timeout_ms", "600000");
        conf.registerKryoClasses(new Class[]{SemanticIndexRow.class});
        String _keySpace = config.getProperty("cassandra.keyspace");
        JavaSparkContext sc = new JavaSparkContext(conf);
        CassandraConnector cassandraConnector = CassandraConnector$.MODULE$.apply(sc.getConf());
        Session session = cassandraConnector.openSession();
        PreparedStatement deleteSemanticIndexRow = session.prepare("DELETE FROM " + _keySpace + ".semantic_index_v2 " +
                "WHERE golem = ? AND random_index = ?");
        PreparedStatement deleteIdiomIndexRow = session.prepare("DELETE FROM " + _keySpace + ".idiom_v2 " +
                "WHERE golem = ? AND random_index = ?");

        JavaPairRDD<Tuple2<Integer, String>, Iterable<CassandraRow>> idiomsGroupedByVector = javaFunctions(sc)
                .cassandraTable(_keySpace, "semantic_index_v2")
                .select("golem", "random_index", "idiom")
                .<Tuple2<Integer, String>>spanBy(
                        row -> new Tuple2(row.getInt("golem"), row.getString("random_index")),
                        classTag(Tuple2.class));

        JavaRDD<SemanticIndexRow> uniqueIdioms = idiomsGroupedByVector
                .map(group -> {
                    Set<String> idiomSample = new HashSet<>(10);
                    int topCount = 10;
                    Iterator<CassandraRow> iter = group._2().iterator();
                    while (iter.hasNext() && topCount > 0){
                        if(idiomSample.add(iter.next().getString("idiom"))){
                            topCount--;
                        }
                    }

                    SemanticIndexRow semanticIndexRow = new SemanticIndexRow();
                    semanticIndexRow.setGolem(group._1()._1());
                    semanticIndexRow.setRandomIndex(group._1()._2());
                    semanticIndexRow.setIdiom(idiomSample.toString());
                    semanticIndexRow.setFrequency(Iterators.size(group._2().iterator()));
                    return semanticIndexRow;
                })
                .filter(row -> row.getFrequency() > 50000);

        javaFunctions(uniqueIdioms)
                .writerBuilder(_keySpace, "idiom_frequency", mapToRow(SemanticIndexRow.class))
                .saveToCassandra();

        // Remove the most frequent mems
        for(int golem: Golem.getValidKeys()){
            List<Row> rows = session.execute(
                    "SELECT random_index FROM " + _keySpace + ".idiom_frequency " +
                            "WHERE golem = " + golem).all();
            // delete from semantic index
            rows.stream()
                    .forEach(row -> {
                        String key = row.getString("random_index");
                        session.execute(deleteSemanticIndexRow.bind(golem, key));
                        log.debug("Deleted from SemanticIndex row golem {}, key {}", golem, key);
                    });
            // delete from idiom index
            rows.stream()
                    .forEach(row -> {
                        String key = row.getString("random_index");
                        session.execute(deleteIdiomIndexRow.bind(golem, key));
                        log.debug("Deleted from IdiomIndex row golem {}, key {}", golem, key);
                    });

        }
    }
}
