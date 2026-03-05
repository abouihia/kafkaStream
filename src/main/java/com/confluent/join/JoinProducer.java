package com.confluent.join;



import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class JoinProducer<T extends SpecificRecord >  implements Closeable {


    private final KafkaProducer<String, T> producer;

    public JoinProducer() {
        this.producer = new KafkaProducer<>(producerProps());
    }

    // ✅ Une seule méthode, plus de doublon
    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }

    public static void main(String[] args) {
        List<ApplianceOrder> applianceOrders = createDataApplianceOrder();
        List<ElectronicOrder> electronicOrders = createDataElectronicOrder();
        List<User> users = getUsers();

       try( JoinProducer joinProducer = new JoinProducer()){
           joinProducer.sendRecords("streams-join-table-input",       users,            u -> ((User)u).getUserId());
           joinProducer.sendRecords("streams-left-side-input",  applianceOrders,   app -> ((ApplianceOrder)app).getUserId());
           joinProducer.sendRecords("streams-right-side-input",  electronicOrders,  el -> ((ElectronicOrder)el).getUserId());
       } catch (IOException e) {
           System.out.println("close with exception :"+e.getMessage());
       }


    }



    // ✅ Générique propre, sans conflit avec le type de classe
    private  void sendRecords(
            String topic,
            List<T> records,
            Function<T , CharSequence> keyExtractor) {

        records.forEach(record -> {
            System.out.println("topic:" + topic + " key :"+keyExtractor.apply(record).toString() + " recored :"+ record);
            ProducerRecord<String, T> producerRecord =
                    new ProducerRecord<>(topic, keyExtractor.apply(record).toString(), record);
            try {
                RecordMetadata metadata = producer.send(producerRecord).get();
                System.out.printf("✅ Sent to [%s] partition=%d offset=%d%n",
                        topic, metadata.partition(), metadata.offset());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // ✅ Bonne pratique obligatoire
                System.err.printf("InterruptedException sending to %s: %s%n", topic, e.getMessage());

            } catch (ExecutionException e) {
                System.err.printf("ExecutionException sending to %s: %s%n", topic, e.getMessage());
            }
        });
    }

    /* ---- Data builders ---- */

    private static List<User> getUsers() {
        return List.of(
                User.newBuilder().setUserId("10261998").setAddress("5405 6th Avenue").setName("Elizabeth Jones").build(),
                User.newBuilder().setUserId("10261999").setAddress("407 64th Street").setName("Art Vandelay").build()
        );
    }

    private static List<ElectronicOrder> createDataElectronicOrder() {
        return List.of(
                ElectronicOrder.newBuilder()
                        .setElectronicId("television-2333").setOrderId("remodel-1")
                        .setUserId("10261998").setTime(Instant.now().toEpochMilli()).build(),
                ElectronicOrder.newBuilder()
                        .setElectronicId("laptop-5333").setOrderId("remodel-2")
                        .setUserId("10261999").setTime(Instant.now().toEpochMilli()).build()
        );
    }

    private static List<ApplianceOrder> createDataApplianceOrder() {
        return List.of(
                ApplianceOrder.newBuilder()
                        .setApplianceId("dishwasher-1333").setOrderId("remodel-1")
                        .setUserId("10261998").setTime(Instant.now().toEpochMilli()).build(),
                ApplianceOrder.newBuilder()
                        .setApplianceId("stove-2333").setOrderId("remodel-2")
                        .setUserId("10261999").setTime(Instant.now().toEpochMilli()).build()
        );
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}