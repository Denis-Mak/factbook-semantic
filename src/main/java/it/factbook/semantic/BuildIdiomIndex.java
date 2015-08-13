package it.factbook.semantic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Properties;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

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
        conf.registerKryoClasses(new Class[]{SemanticIndexRow.class});
        String _keySpace = config.getProperty("cassandra.keyspace");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<SemanticIndexRow> uniqueIdioms = javaFunctions(sc)
                .cassandraTable(_keySpace, "semantic_index_v2")
                .select("golem", "random_index", "idiom")
                .map(row -> {
                    SemanticIndexRow semanticIndexRow = new SemanticIndexRow();
                    semanticIndexRow.setGolem(row.getInt(0));
                    semanticIndexRow.setRandomIndex(row.getString(1));
                    semanticIndexRow.setIdiom(row.getString(2));
                    return semanticIndexRow;
                })
                .distinct();

        javaFunctions(uniqueIdioms)
                .writerBuilder(_keySpace, "idiom_v2", mapToRow(SemanticIndexRow.class))
                .saveToCassandra();

        System.out.println("Job is done.");
    }
}
