package com.confluent.processor;

import com.confluent.join.JoinProducer;
import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ProcessorProducer<T> implements Closeable {


    private KafkaProducer<String, T> producer ;


    public  ProcessorProducer(){
        this.producer = new KafkaProducer<>(producerProps(KafkaAvroSerializer.class.getName()));
    }

    private Map<String, Object> producerProps(String  name ) {

        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, name);
        propsMap.put(  "schema.registry.url", "http://localhost:8081");
        return  propsMap;
    }

    public static void main(String[] args) {
        List<ElectronicOrder> electronicOrders = createDataElectronicOrder();

        try( ProcessorProducer processorProducer = new ProcessorProducer()){
            processorProducer.sendRecords(
                                "processor-input-topic",
                                      electronicOrders,
                                el -> ((ElectronicOrder)el).getTime() ,
                                el -> ((ElectronicOrder)el).getUserId());
        } catch (IOException e) {
            System.out.println("close with exception :"+e.getMessage());
        }


    }


    // ✅ Générique propre, sans conflit avec le type de classe
    private  void sendRecords(
            String topic,
            List<T> records,
            Function<T , Long> keyExtractorTime,
            Function<T , CharSequence> keyExtractor) {

        records.forEach(record -> {
            System.out.println("topic:" + topic + " key :"+keyExtractor.apply(record).toString() + " recored :"+ record);
            ProducerRecord<String, T> producerRecord =
                    new ProducerRecord<>(topic, 0, keyExtractorTime.apply(record), keyExtractor.apply(record).toString(), record);
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



    @Override
    public void close() throws IOException {
          producer.close();
    }

    private static List<ElectronicOrder> createDataElectronicOrder() {

        Instant instant = Instant.now();

        ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("10261998")
                .setPrice(2000.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);

        ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("1033737373")
                .setPrice(1999.23)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);

        ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("1026333")
                .setPrice(4500.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);

        ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("1038884844")
                .setPrice(1333.98)
                .setTime(instant.toEpochMilli()).build();


       return List.of(electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour );
    }

}
