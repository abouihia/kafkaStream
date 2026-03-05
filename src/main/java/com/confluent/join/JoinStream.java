package com.confluent.join;

import com.confluent.StreamsUtils;

import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.CombinedOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.confluent.StreamsUtils.getSpecificAvroSerde;
import static com.confluent.StreamsUtils.propertiesToMap;

public class JoinStream {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "joining-streams";

    static void main(String[] args) throws IOException {

        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);


        String streamOneInput = streamsProps.getProperty("stream_one.input.topic");
        String streamTwoInput = streamsProps.getProperty("stream_two.input.topic");
        String tableInput = streamsProps.getProperty("table.input.topic");
        String outputTopic = streamsProps.getProperty("joins.output.topic");

        Map<String, Object> configMap   = propertiesToMap(streamsProps);
        SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);

        List<NewTopic> newTopicList =List.of(   StreamsUtils.createTopic(streamOneInput),
                                                StreamsUtils.createTopic(streamTwoInput),
                                                StreamsUtils.createTopic(tableInput),
                                                StreamsUtils.createTopic(outputTopic));

        createTopicsAndSendData(streamsProps , newTopicList);



        Topology joinTopology = topologyJoin( streamOneInput,
                                             streamTwoInput,
                                             tableInput,
                                             outputTopic,
                                             configMap ,
                                            electronicSerde,
                                             applianceSerde,
                                             userSerde,
                                            combinedSerde);

        //3- create an instance of kafkaStreams
        StreamsUtils.createStream(streamsProps,joinTopology);

    }



    public static Topology topologyJoin(final String streamOneInput ,
                                        String streamTwoInput,
                                        String tableInput,
                                        String outputTopic,
                                        Map<String, Object> configMap ,
                                        SpecificAvroSerde<ElectronicOrder> electronicSerde,
                                        SpecificAvroSerde<ApplianceOrder> applianceSerde,
                                        SpecificAvroSerde<User> userSerde,
                                        SpecificAvroSerde<CombinedOrder> combinedSerde){


        /* les values joiner*/
        ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                        .setApplianceOrderId(applianceOrder.getOrderId())
                        .setApplianceId(applianceOrder.getApplianceId())
                        .setElectronicOrderId(electronicOrder.getOrderId())
                        .setTime(Instant.now().toEpochMilli())
                        .build();

        ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {  combined.setUserName(user.getName());}
            return combined;
        };




        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, ApplianceOrder> applianceStream =
                streamsBuilder
                        .stream(streamOneInput, Consumed.with(Serdes.String(), applianceSerde))
                        .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

        KStream<String, ElectronicOrder> electronicStream =
                streamsBuilder
                        .stream(streamTwoInput, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Electronic stream incoming record " + key + " value " + value));

        KTable<String, User> userTable =
                streamsBuilder.table(tableInput, Materialized.with(Serdes.String(), userSerde));

        //join les deux streams:
        //1- jointure classic
        KStream<String, CombinedOrder> combinedStream =
                applianceStream.join(
                                electronicStream,
                                orderJoiner,
                                JoinWindows.of(Duration.ofMinutes(30)),
                                StreamJoined.with(Serdes.String(), applianceSerde, electronicSerde))
                        .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));

        //2- jointure left
        combinedStream.leftJoin(
                        userTable,
                        enrichmentJoiner,
                        Joined.with(Serdes.String(), combinedSerde, userSerde))
                .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), combinedSerde));

        return  streamsBuilder.build();
    }


    //OK
    public static void createTopicsAndSendData(final Properties properties,  List<NewTopic>  newTopicList) {

        Properties properties1 = new Properties();
        properties1.putAll(properties);
        properties1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try (
            AdminClient adminClient = AdminClient.create(properties1)) {
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
