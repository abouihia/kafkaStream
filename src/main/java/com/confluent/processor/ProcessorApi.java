package com.confluent.processor;

import com.confluent.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.confluent.StreamsUtils.propertiesToMap;

public class ProcessorApi {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "processor-api-application";

    static void main(String[] args) throws IOException {

        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);

        final String inputTopic = streamsProps.getProperty("processor.input.topic");
        final String outputTopic = streamsProps.getProperty("processor.output.topic");

        //2- creation de topics
        var newTopicList = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
        createTopicsAndSendData(streamsProps , newTopicList);


        //3- create an instance of kafkaStreams
        Topology topologyKtable = topologyProcessor(  streamsProps, inputTopic,  outputTopic);
        StreamsUtils.createStream(streamsProps,topologyKtable);

    }

  static   Topology  topologyProcessor( Properties streamsProps, String inputTopic, String outputTopic){

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();
        final  String storeName = "total-price-store";
        Map<String, Object> configMap   = propertiesToMap(streamsProps);
        final SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamsUtils.getSpecificAvroSerde(configMap);

        final Topology topology = new Topology();
        topology.addSource(
                "source-node",
                stringSerde.deserializer(),
                electronicSerde.deserializer(),
                inputTopic);

        topology.addProcessor(
                "aggregate-price",
                new TotalPriceOrderProcessorSupplier(storeName),
                "source-node");

        topology.addSink(
                "sink-node",
                outputTopic,
                stringSerde.serializer(),
                doubleSerde.serializer(),
                "aggregate-price");



        return  topology;
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
}
