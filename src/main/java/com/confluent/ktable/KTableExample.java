package com.confluent.ktable;

import com.confluent.StreamsUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KTableExample {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "ktable-application";

    static void main(String[] args) throws IOException {

        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);

        String inputTopic = streamsProps.getProperty("ktable.input.topic");
        String outputTopic = streamsProps.getProperty("ktable.output.topic");

        //2- creation de topics
        var newTopicList = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
        createTopicsAndSendData(streamsProps , newTopicList);


        //3- create an instance of kafkaStreams
        Topology topologyKtable = topologyKtable( inputTopic,  outputTopic);
        StreamsUtils.createStream(streamsProps,topologyKtable);

    }


    //OK
    public static void createTopicsAndSendData(final Properties properties,  List<NewTopic> newTopicList) {

        Properties properties1 = new Properties();
        properties1.putAll(properties);
        properties1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (AdminClient adminClient = AdminClient.create(properties1)) {
            var createTopicResult = adminClient.createTopics(newTopicList);
            try {
                createTopicResult.all().get();
                System.out.println("for KTable  :topics are created successfully  ");
            } catch (Exception e) {
                System.out.println("for KTable  :Exception creating topics :  " + e.getMessage());
            }
        }
    }

    public static Topology topologyKtable(String inputTopic, String outputTopic){

        final String orderNumberStart = "orderNumber-";
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> firstKTable = streamsBuilder.table(inputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        firstKTable.filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));



        return  streamsBuilder.build();
    }
}
