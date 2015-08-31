package it.factbook.semantic;

import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.classTag;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class BuildIdiomIndex {

    public static void main(String[] args) {
        Properties config = Util.readProperties();

        SparkConf conf = new SparkConf();
        conf.setAppName("Build Idiom Index");
        conf.setMaster(config.getProperty("spark.master"));
        conf.set("spark.cassandra.connection.host", config.getProperty("cassandra.host"));
        conf.set("spark.cassandra.auth.username", config.getProperty("cassandra.user"));
        conf.set("spark.cassandra.auth.password", config.getProperty("cassandra.password"));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "true");
        conf.set("spark.kryoserializer.buffer.max", "512M");
        conf.set("spark.executor.memory", "8G");
        //conf.set("spark.default.parallelism", "4");
        //conf.set("spark.cassandra.input.split.size_in_mb", "10");
        conf.registerKryoClasses(new Class[]{SemanticIndexRow.class});
        String _keySpace = config.getProperty("cassandra.keyspace");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Tuple3<Integer, String, String>, Iterable<CassandraRow>> idiomsGroupedByVector = javaFunctions(sc)
                .cassandraTable(_keySpace, "semantic_index_v2")
                .select("golem", "random_index", "mem", "idiom")
                .<Tuple3<Integer, String, String>>spanBy(
                        row -> new Tuple3(row.getInt("golem"), row.getString("random_index"), row.getString("mem")),
                        classTag(Tuple3.class));
        JavaRDD<SemanticIndexRow> uniqueIdioms = idiomsGroupedByVector
                .map(group -> {
                    Set<String> idiomsOfTheGroup = new HashSet<>();
                    group._2().forEach(row -> idiomsOfTheGroup.add(row.getString("idiom")));
                    SemanticIndexRow semanticIndexRow = new SemanticIndexRow();
                    semanticIndexRow.setGolem(group._1()._1());
                    semanticIndexRow.setRandomIndex(group._1()._2());
                    semanticIndexRow.setMem(group._1()._3());
                    semanticIndexRow.setIdiom(idiomsOfTheGroup.stream().collect(Collectors.joining("; ")));
                    return semanticIndexRow;
                });
        javaFunctions(uniqueIdioms)
                .writerBuilder(_keySpace, "idiom_v2", mapToRow(SemanticIndexRow.class))
                .saveToCassandra();
    }
}
