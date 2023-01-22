package iust.lab;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
//
public class Consumer {
    public static void main(String... strs) throws InterruptedException, StreamingQueryException, TimeoutException, IOException, URISyntaxException {

//        SparkSession sparkSession = SparkSession.builder().master("local[3]")
//                .appName("name").getOrCreate();
        StreamingContext streamingContext = new JavaStreamingContext("local[3]", "name", Durations.seconds(1));
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
//        sparkSession.udf().register("uuid", (String s) -> UUID.randomUUID().toString()
//                , org.apache.spark.sql.types.DataTypes.StringType);
//        sparkSession.udf().register("ba2i", (byte[] x) -> ByteBuffer.wrap(x).getInt() * 2,
//                org.apache.spark.sql.types.DataTypes.IntegerType);
        Dataset<org.apache.spark.sql.Row> df = sparkSession.readStream().format("kafka").
                option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test").load();
//        df.map(r -> r.get)
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(df.rdd(), numClusters, numIterations);
//        df.drop("timestampType", "timestamp", "offset", "partition",
//                        "key", "topic").withColumn("id", functions.callUDF("uuid", functions.lit("a")))
//                .withColumnRenamed("value", "sparkres").withColumn("sparkres", functions.callUDF("ba2i", functions.col("sparkres")))
//                .writeStream()
//                .option("keyspace", "default")
//                .option("table", "test_tb")
//                .option("checkpointLocation", "/home/isiran")
//                .option("spark.cassandra.auth.username", "cassandra")
//                .option("spark.cassandra.auth.password", "cassandra")
//                .outputMode("append")
//                .format("org.apache.spark.sql.cassandra").start();
    }
}
