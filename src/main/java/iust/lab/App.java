package iust.lab;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import iust.lab.model.Point;
import iust.lab.utils.CsvReader;
import iust.lab.utils.KafkaPointProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.Path.SEPARATOR;

public class App {
    private static final String SPLITTER = ",";
    private static final String FILE_PATH = "src" + SEPARATOR + "main" + SEPARATOR + "resources" + SEPARATOR +
            "09-18-20_adsb" + SEPARATOR + "1.csv";
    private static final String SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "point-data";
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    public static void main(String[] args) throws IOException {

        final File csvFile = new File(FILE_PATH);
        if (!csvFile.exists()) {
            throw new FileNotFoundException("File not found");
        }
        final ImmutableList<Point> pointDataList = ImmutableList
                .copyOf(
                        new CsvReader(SPLITTER)
                                .loadCsvContentToList(new BufferedReader(new FileReader(csvFile)))
                );
        if (pointDataList.size() == 0) {
            System.out.println("No Data Found in File");
            return;
        }
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final KafkaPointProducer kafkaPointProducer =
                new KafkaPointProducer(
                        producer,
                        TOPIC_NAME);
        pointDataList.forEach(point -> {
            try {
                System.out.println(point);
                kafkaPointProducer.send(mapper.writeValueAsString(point));
                System.out.println("sent");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        kafkaPointProducer.flush();
        kafkaPointProducer.close();
    }
}
