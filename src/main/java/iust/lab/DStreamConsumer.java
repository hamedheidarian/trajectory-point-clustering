package iust.lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import iust.lab.model.Point;
import iust.lab.serialization.PointDeserializer;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class DStreamConsumer {

    public static void main(String... args) throws InterruptedException {
        String brokers = "localhost:9092";
        String groupId = "100";
        String topics = "point-data";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaKMeans").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PointDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, Point>> data = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
        val trainingData = data.map(record -> {
            val point = record.value();
            return getVector(point);
        });
        StreamingKMeans streamingKmeans = new StreamingKMeans().setK(3).setDecayFactor(1.0).setRandomCenters(5,0.0,0);
        streamingKmeans.trainOn(trainingData);
        val testData = data.map(record -> LabeledPoint
                .apply(Double.parseDouble(String.valueOf(record.value().getId())),
        getVector(record.value())));
        streamingKmeans.predictOnValues(testData.mapToPair((PairFunction<LabeledPoint, Double, Vector>) arg0
                -> new Tuple2<>(arg0.label(), arg0.features()))).print();
        jssc.start();
        jssc.awaitTermination();
    }

    private static Vector getVector(Point point) {
        double[] features 
                = {point.getDateTime(), point.getAltitude(), point.getSpeed(), point.getLat(), point.getLon()};
        return Vectors.dense(features);
    }
}