package com.withoutspring.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.Instant.now;


@Slf4j
public class ProducerUtil {


    static KafkaProducer<String, String> producer = new KafkaProducer<String, String >(producerProps());

    private static Map<String, Object> producerProps() {

        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return  propsMap;
    }

   public static RecordMetadata publishMessageSync(String  topicName, String key, String message){

       ProducerRecord<String, String> producerRecord  = new ProducerRecord<>(topicName, key, message);
       RecordMetadata recordMetadata  = null;
       try {
           System.out.printf("producerRecord : " + producerRecord);
           recordMetadata = producer.send(producerRecord).get();
       } catch (InterruptedException e) {
           System.out.printf("InterruptedException in  publishMessageSync : {}  ", e.getMessage(), e);
       } catch (ExecutionException e) {
           System.out.printf("ExecutionException in  publishMessageSync : {}  ", e.getMessage(), e);
       }catch(Exception e){
           System.out.printf("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
       }
       return recordMetadata;
   }

    public static RecordMetadata publishMessageSyncWithDelay(String  topicName, String key, String message , long delay ){

        ProducerRecord<String, String> producerRecord  =
                    new ProducerRecord<>(topicName, 0, now().plusSeconds(delay).toEpochMilli(), key,  message);
        RecordMetadata recordMetadata  = null;
        try {
            System.out.printf("producerRecord : " + producerRecord);
            recordMetadata = producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            System.out.printf("InterruptedException in  publishMessageSync : {}  ", e.getMessage(), e);
        } catch (ExecutionException e) {
            System.out.printf("ExecutionException in  publishMessageSync : {}  ", e.getMessage(), e);
        }catch(Exception e){
            System.out.printf("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
        }
        return recordMetadata;
    }

    public static void  publishMessagesWithDelay(Map<String, String> alphabetMap, String topic, int delaySeconde) {

        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = publishMessageSyncWithDelay(topic, key,value,delaySeconde);
                    System.out.printf("Published the alphabet message : {} ", recordMetaData);
                });
    }

    public static void publishMessages(Map<String, String> alphabetMap, String topic) {

        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = publishMessageSync(topic, key,value);
                    System.out.printf("Published the alphabet message : {} ", recordMetaData);
                });
    }


}
