package iust.lab;
import org.apache.spark.api.java.JavaRDD;
import com.datastax.driver.core.*;
import java.util.stream.Collectors;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import iust.lab.db.CassandraConnector;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.functions;

public class Consumer {
    protected static final String out = null;

    public static void main(String... strs) throws InterruptedException, StreamingQueryException, TimeoutException, IOException, URISyntaxException {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042);
        try(Session session =  connector.getSession()) {
                String keyspace = "CREATE  KEYSPACE IF NOT EXISTS default  \n" +
                        "   WITH REPLICATION = { \n" +
                        "      'class' : 'SimpleStrategy', 'replication_factor' : 1 } ";
                session.execute(keyspace);
                StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append("default.test_tb").append("(")
                        .append("id uuid PRIMARY KEY, ")
                        .append("sparkRes bigint").append(")");
                String query = sb.toString();
                session.execute(query);
    
        }    
        
        SparkSession sparkSession = SparkSession.builder().master("local[3]")
        .appName("name").getOrCreate();
        sparkSession.udf().register("uuid", (String s) -> UUID.randomUUID().toString()
        , org.apache.spark.sql.types.DataTypes.StringType);
        sparkSession.udf().register("ba2i", (byte[] x) -> ByteBuffer.wrap(x).getInt() * 2,
        org.apache.spark.sql.types.DataTypes.IntegerType );
        Dataset<org.apache.spark.sql.Row> df = sparkSession.readStream().format("kafka").
        option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test").load();
        df.drop("timestampType", "timestamp", "offset", "partition",
        "key", "topic").withColumn("id", functions.callUDF("uuid", functions.lit("a")))
        .withColumnRenamed("value", "sparkres").withColumn("sparkres", functions.callUDF("ba2i", functions.col("sparkres")))
        .writeStream()
        .option("keyspace", "default")
            .option("table", "test_tb")
            .option("checkpointLocation", "/home/isiran")
            .option("spark.cassandra.auth.username", "cassandra")
            .option("spark.cassandra.auth.password", "cassandra")
            .outputMode("append")
            .format("org.apache.spark.sql.cassandra").start();
        }
    }
