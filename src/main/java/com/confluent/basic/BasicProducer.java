package com.confluent.basic;


import com.confluent.join.JoinProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BasicProducer<T>  implements Closeable {

    private KafkaProducer<String, T> producer ;


    public  BasicProducer(){
        this.producer = new KafkaProducer<>(producerProps(StringSerializer.class.getName()));
    }

    private Map<String, Object> producerProps(String  name ) {

        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, name);
        propsMap.put(  "schema.registry.url", "http://localhost:8081");
        return  propsMap;
    }

    public static void main() {


        List<String> rawRecords = List.of("orderNumber-1001",
                "orderNumber-5000",
                "orderNumber-999",
                "orderNumber-3330",
                "bogus-1",
                "bogus-2",
                "orderNumber-8400");


        try(  BasicProducer<String>  stringBasicProducer  = new BasicProducer<>()){
            stringBasicProducer.publishElementGeneric(rawRecords, "basic-input-streams");
        }catch (IOException e) {
            System.out.println("close with exception :"+e.getMessage());
        }

    }


    private   void publishElementGeneric(List<T> listOfRecords , String inputTopic ) {

        listOfRecords.stream()
                .forEach(word -> {
                    ProducerRecord<String, T> producerRecord  = new ProducerRecord<>(inputTopic, "order-key", word);
                    RecordMetadata recordMetadata  = null;
                    try {
                        System.out.printf("producerRecord : " + producerRecord);
                        recordMetadata = this.producer.send(producerRecord).get();
                        System.out.printf("✅ Sent to [%s] partition=%d offset=%d%n", inputTopic, recordMetadata.partition(), recordMetadata.offset());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // ✅ Bonne pratique obligatoire
                        System.out.printf("InterruptedException in  publishMessageSync : {}  ", e.getMessage(), e);
                    } catch (ExecutionException e) {
                        System.out.printf("ExecutionException in  publishMessageSync : {}  ", e.getMessage(), e);
                    }catch(Exception e){
                        System.out.printf("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
                    }
                });
    }



    @Override
    public void close() throws IOException {
        producer.close();
    }
}
