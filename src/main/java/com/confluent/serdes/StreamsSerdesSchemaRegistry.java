package com.confluent.serdes;

import com.confluent.StreamsUtils;
import com.withoutspring.utils.LauncherUtils;
import io.confluent.developer.avro.ProcessedOrder;
import io.confluent.developer.avro.ProductOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.confluent.StreamsUtils.*;

public class StreamsSerdesSchemaRegistry {

    public static final String APPLICATION_ID_FOR_MY_STREAM = "schema-registry-streams";

    static void main(String[] args) throws IOException {


        //1-properties
        final Properties streamsProps = StreamsUtils.loadProperties(APPLICATION_ID_FOR_MY_STREAM);

        //2- créer les topics and publish
        final String inputTopic = streamsProps.getProperty("sr.input.topic");
        final String outputTopic = streamsProps.getProperty("sr.output.topic");
        createTopicsAndSendData(streamsProps,  inputTopic, outputTopic);

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(streamsProps, topologyRegistry( streamsProps,  inputTopic, outputTopic));

    }


    public static Topology topologyRegistry(Properties streamsProps ,  final String inputTopic , String outputTopic){

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        final Map<String, String> configMap = propertiesToMapString(streamsProps);
        final SpecificAvroSerde<ProductOrder> productOrderSerde = getSpecificAvroSerdeString(configMap);
        final SpecificAvroSerde<ProcessedOrder> processedOrderSerde = getSpecificAvroSerdeString(configMap);

        final KStream<String, ProductOrder> orderStream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), productOrderSerde));
        orderStream.mapValues(value -> ProcessedOrder.newBuilder()
                        .setProduct(value.getProduct())
                        .setTimeProcessed(Instant.now().toEpochMilli()).build())
                .peek((key, value) -> System.out.println("Brahim record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), processedOrderSerde));


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
