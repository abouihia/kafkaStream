package com.confluent;

import com.withoutspring.exception.StreamProcessorCustomErrorHandler;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsUtils {

    public static final short REPLICATION_FACTOR = 1;
    public static final int PARTITIONS = 2;




    public static Properties loadProperties(String applicationId) throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            properties.load(fis);
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG,  applicationId);
            return properties;
        }
    }

    public static NewTopic createTopic(final String topicName){
        return new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
    }

    public static Callback callback() {
        return (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }

        };
    }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

   public  static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerdeString(final Map<String, String> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static Map<String,Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> {
            configs.put((String)key, (String)value);
        });
        return configs;
    }

   public  static Map<String, String> propertiesToMapString(final Properties properties) {
        final Map<String, String> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String) key, (String) value));
        return configs;
    }

    public static void createStream(Properties config, Topology topology) {

        try {
            //3- créer le stream Kafka  avec son topology
            var kafkastreams  = new KafkaStreams(topology, config);
            //when want to customiser  topology exception
            kafkastreams.setUncaughtExceptionHandler(new StreamProcessorCustomErrorHandler());
            Runtime.getRuntime().addShutdownHook(new Thread(kafkastreams::close));
            kafkastreams.start();
            IO.println("is started");
        } catch (Exception e) {
            System.out.println("Exceptions in starting the streams :{}" +e.getMessage());
        }
    }


}
