package com.confluent.error;

import com.confluent.StreamsUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class StreamsErrorHandling {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "streams-error-handling";

    static boolean throwErrorNow = true;

    static void main(String[] args) throws IOException {

        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,  StreamsDeserializationErrorHandler.class);
        streamsProps.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsRecordProducerErrorHandler.class);

        //2- créer les topics and publish
        final String inputTopic = streamsProps.getProperty("error.input.topic");
        final String outputTopic = streamsProps.getProperty("error.output.topic");
        createTopicsAndSendData(streamsProps,  inputTopic, outputTopic);


        //3- create an instance of kafkaStreams
        Topology aggregateTopology =  topologyError( streamsProps);
        StreamsUtils.createStream(streamsProps,aggregateTopology);
    }


    public static Topology topologyError(final Properties streamsProps) {
        StreamsBuilder builder = new StreamsBuilder();

        final String orderNumberStart = "orderNumber-";
        KStream<String, String> streamWithErrorHandling =
                                  builder
                                          .stream(streamsProps.getProperty("error.input.topic"), Consumed.with(Serdes.String(), Serdes.String()))
                                         .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        streamWithErrorHandling
                .filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> {
                    if (throwErrorNow) {
                        throwErrorNow = false;
                        throw new IllegalStateException("Retryable transient error");
                    }
                    return value.substring(value.indexOf("-") + 1);
                })
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(streamsProps.getProperty("error.output.topic"), Produced.with(Serdes.String(), Serdes.String()));



        return  builder.build();
    }


    public static void createTopicsAndSendData(final Properties properties, String inputTopic, String outputTopic) {

        Properties properties1  = new Properties();
        properties1.putAll(properties);
        properties1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

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
}
