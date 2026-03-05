package com.confluent.time;

import com.confluent.StreamsUtils;
import com.withoutspring.utils.LauncherUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StreamsTimestampExtractor {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "extractor-windowed-streams";


    static void main(String[] args) throws IOException {


        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);

        //2- créer les topics and publish
        final String inputTopic = streamsProps.getProperty("extractor.input.topic");
        final String outputTopic = streamsProps.getProperty("extractor.output.topic");
        createTopicsAndSendData(streamsProps, inputTopic, outputTopic);

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(streamsProps, topologyTimeStampExtractor(streamsProps,   inputTopic, outputTopic));
    }

    public static Topology topologyTimeStampExtractor(Properties streamsProps ,  final String inputTopic , String outputTopic){


        final Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamsUtils.getSpecificAvroSerde(configMap);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, ElectronicOrder> electronicStream =
                             streamsBuilder
                                .stream(inputTopic,Consumed.with(Serdes.String(), electronicSerde) .withTimestampExtractor(new OrderTimestampExtractor()))
                                .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        electronicStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                .aggregate(() -> 0.0,  (key, order, total) -> total + order.getPrice(), Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(), value))
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));



        return  streamsBuilder.build();
    }


    public static void createTopicsAndSendData(final Properties properties, String inputTopic, String outputTopic) {
        Properties properties1 = new Properties();
        properties1.putAll(properties);
        properties1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try (AdminClient adminClient = AdminClient.create(properties1)) {

            List<NewTopic> newTopicList = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            var createTopicResult = adminClient.createTopics(newTopicList);
            try {
                createTopicResult.all().get();
                System.out.println("topics are created successfully");
            } catch (Exception e) {
                System.out.println("Exception creating topics :  " + e.getMessage());
            }

        }
    }
}
