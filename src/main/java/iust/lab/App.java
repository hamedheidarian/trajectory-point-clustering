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
import java.util.Properties;

import static org.apache.hadoop.fs.Path.SEPARATOR;

/**
 * Hello world!
 */
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
        final KafkaPointProducer kafkaInsuranceProducer =
                new KafkaPointProducer(
                        producer,
                        TOPIC_NAME);
        pointDataList.forEach(policy -> {
            try {
                kafkaInsuranceProducer.send(mapper.writeValueAsString(policy));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } finally {
                kafkaInsuranceProducer.close();
            }
        });
    }


//    public static void main(String[] args) throws IOException, CsvValidationException {
//        List<List<String>> records = new ArrayList<>();
//        try (CSVReader csvReader = new CSVReader(new FileReader(dataFile))) {
//            String[] values;
//            while ((values = csvReader.readNext()) != null) {
//                System.out.println(Arrays.toString(values));
//                records.add(Arrays.asList(values));
//            }
//        }

//        System.out.println(records);

//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
//        KafkaProducer<Integer, Integer> kafkaProducer = new KafkaProducer<>(props);
    //IntStream randoms = ThreadLocalRandom.current().ints();
//        ProducerRecord<Integer, Integer> record = new ProducerRecord<>("test",
//                1400);
//            RecordMetadata metadata = kafkaProducer.send(record).get();
    // System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
    //        + " with offset " + metadata.offset());
//    }
}
