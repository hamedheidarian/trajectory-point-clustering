package iust.lab;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.clustering.StreamingKMeansModel;
import org.apache.spark.sql.*;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//
public class Consumer {
    public static void main(String... strs) throws StreamingQueryException {

        SparkSession spark = SparkSession.builder().master("local[3]")
                .appName("name").getOrCreate();
//        StreamingContext streamingContext = new JavaStreamingContext("local[3]", "name", Durations.seconds(1));
//        JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(
//                        streamingContext,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//                );
////        sparkSession.udf().register("uuid", (String s) -> UUID.randomUUID().toString()
////                , org.apache.spark.sql.types.DataTypes.StringType);
////        sparkSession.udf().register("ba2i", (byte[] x) -> ByteBuffer.wrap(x).getInt() * 2,
////                org.apache.spark.sql.types.DataTypes.IntegerType);
//        Dataset<org.apache.spark.sql.Row> df = sparkSession.readStream().format("kafka").
//                option("kafka.bootstrap.servers", "localhost:9092")
//                .option("subscribe", "test").load();
////        df.map(r -> r.get)
//        int numClusters = 2;
//        int numIterations = 20;
//        KMeansModel clusters = KMeans.train(df.rdd(), numClusters, numIterations);
////        df.drop("timestampType", "timestamp", "offset", "partition",
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


//        SparkSession spark = SparkSession.builder()
//                .appName("KafkaClustering")
//                .getOrCreate();

        // define a kafka topic to consume from
        String topic = "point-data";

        // consume data from the kafka topic and store it in a DataFrame
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("lat", DataTypes.FloatType, false),
                DataTypes.createStructField("lon", DataTypes.FloatType, false),
                DataTypes.createStructField("altitude", DataTypes.IntegerType, false),
                DataTypes.createStructField("speed", DataTypes.IntegerType, false),
                DataTypes.createStructField("dateTime", DataTypes.LongType, false) });
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topic)
                .load()
                .selectExpr("CAST(value AS String)")
                .select(functions.from_json(new Column("value"), schema).as("data"))
                .select("data.*");

        // define the columns to use for clustering
        String[] features = {"lat", "altitude", "dateTime", "speed"};

        // prepare the data for clustering by assembling the feature columns into a single vector column
        
        df.writeStream().foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>) (dataset, batchId) -> {
                    VectorAssembler assembler = new VectorAssembler()
                            .setInputCols(features)
                            .setOutputCol("features");
                    dataset = assembler.transform(dataset);
                    KMeans kmeans = new KMeans().setK(3).setSeed(1);
                    KMeansModel model = kmeans.fit(dataset);

                    // predict the cluster assignments for new data
                    Dataset<Row> predictions = model.transform(dataset);

                    // display the cluster assignments
                    predictions.select("prediction").show();
                }
        ).start();



        // Scale the data
//        StandardScaler scaler = new StandardScaler()
//                .setInputCol("features")
//                .setOutputCol("scaledFeatures")
//                .setWithStd(true)
//                .setWithMean(false);
//        df = scaler.fit(df).transform(df);


        // train a K-Means clustering model

    }
}
