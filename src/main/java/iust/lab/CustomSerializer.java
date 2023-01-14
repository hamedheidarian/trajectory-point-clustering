//package iust.lab;
//
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.common.serialization.Serializer;
//
//import java.util.Map;
//
//public class CustomSerializer implements Serializer<Integer> {
//
//    public void configure(Map<String, ?> configs, boolean isKey) {
//
//    }
//
//
//    public byte[] serialize(String topic, Integer data) {
//        byte[] retVal = null;
//        ObjectMapper objectMapper = new ObjectMapper();
//        try {
//            retVal = objectMapper.writeValueAsString(data).getBytes();
//        } catch (Exception exception) {
//            System.out.println("Error in serializing object"+ data);
//        }
//        return retVal;
//    }
//
//    public void close() {
//
//    }
//
//}