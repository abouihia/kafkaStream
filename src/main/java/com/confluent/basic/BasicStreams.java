package com.confluent.basic;

import com.confluent.StreamsUtils;
import com.withoutspring.utils.LauncherUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.List;
import java.util.Properties;



public class BasicStreams {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "basic-streams";


    static void main(String[] args) throws IOException {


        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);

        //2- créer les topics and publish
        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.topic");
        createTopicsAndSendData(streamsProps,  inputTopic, outputTopic);

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(streamsProps,topologyBasic( inputTopic, outputTopic));
    }

    public static void createTopicsAndSendData(final Properties properties, String inputTopic, String outputTopic) {
        Properties properties1 = new Properties();
        properties1.putAll(properties);
        properties1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

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

    public static Topology topologyBasic(  final String inputTopic , String outputTopic){

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        final String orderNumberStart = "orderNumber-";

      streamsBuilder
               .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value))
                .filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return  streamsBuilder.build();
    }
}
