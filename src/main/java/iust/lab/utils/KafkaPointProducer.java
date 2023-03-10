package iust.lab.utils;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class KafkaPointProducer {
    final Producer<String, String> producer;
    private final String topicName;

    public KafkaPointProducer(final Producer<String, String> producer,
                              final String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    public void close() {
        this.producer.close();
    }

    public void send(final String message) throws ExecutionException, InterruptedException {
        RecordMetadata rm = producer.send(new ProducerRecord<>(this.topicName, message)).get();
        System.out.println(rm);
    }

    public void flush() {
        producer.flush();
    }
}
