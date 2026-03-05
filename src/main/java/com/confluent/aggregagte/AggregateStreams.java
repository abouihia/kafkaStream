package com.confluent.aggregagte;

import com.confluent.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
public class AggregateStreams {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "aggregate-streams";

    static void main(String[] args) throws IOException {


        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);

        //2- créer les topics and publish
        final String inputTopic = streamsProps.getProperty("aggregate.input.topic");
        final String outputTopic = streamsProps.getProperty("aggregate.output.topic");
        createTopicsAndSendData(streamsProps,  inputTopic, outputTopic);


        //3- create an instance of kafkaStreams
        Topology  aggregateTopology =  topologyAggergator( streamsProps);
        StreamsUtils.createStream(streamsProps,aggregateTopology);
    }






    public static void createTopicsAndSendData(final Properties properties, String inputTopic, String outputTopic) {

        Properties properties1  = new Properties();
        properties1.putAll(properties);
        properties1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

       try(AdminClient adminClient = AdminClient.create(properties1)){
           var topics = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
           
           var createTopicResult =  adminClient.createTopics(topics);;
           try {
               createTopicResult.all().get();
               System.out.println("topics are created successfully");
           } catch (Exception e) {
               System.out.println("Exception creating topics :  "+e.getMessage());
           }
       }
       
    }



    public static Topology topologyAggergator(final Properties streamsProps) {
        final Map<String, Object> streamsPropsMap = StreamsUtils.propertiesToMap(streamsProps);
        final SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamsUtils.getSpecificAvroSerde(streamsPropsMap);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Aggregator<String, ElectronicOrder, Double>  aggregator  =
                (key, order, total) -> total + order.getPrice();


        final KStream<String, ElectronicOrder> electronicStream =
                streamsBuilder.stream( streamsProps.getProperty("aggregate.input.topic"), Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        electronicStream.groupByKey().aggregate(() -> 0.0,
                        (key, order, total) -> total + order.getPrice(),
                        Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to( streamsProps.getProperty("aggregate.output.topic"), Produced.with(Serdes.String(), Serdes.Double()));



        return streamsBuilder.build();
    }

}
