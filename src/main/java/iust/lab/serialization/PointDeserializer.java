package iust.lab.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import iust.lab.model.Point;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PointDeserializer implements Deserializer<Point> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Point deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                System.out.println("Null received at deserializing");
                return null;
            }
            return objectMapper.readValue(new String(data, "UTF-8"), Point.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing String to Point");
        }
    }

    @Override
    public void close() {
    }
}
