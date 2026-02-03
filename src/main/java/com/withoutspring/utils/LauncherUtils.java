package com.withoutspring.utils;


import com.withoutspring.exception.StreamProcessorCustomErrorHandler;import com.withoutspring.topology.GreetingsTopology;
import com.withoutspring.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class LauncherUtils {


   public static final  List<String> ordersTopics = List.of(OrdersTopology.ORDERS, OrdersTopology.GENERAL_ORDERS, OrdersTopology.RESTAURANT_ORDERS);

    public static final  List<String> greetingTopics = List.of(GreetingsTopology.GREETINGS_ENGILSH,GreetingsTopology.GREETINGS,  GreetingsTopology.GREETINGS_UPPERCASE);

    public static Properties   initConfigProperties(String applicationId) {

        Properties config  = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");
        return  config;


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
            log.error("Exceptions in starting the streams :{}", e.getMessage(), e);
        }
    }


    public static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);

        var partitions = 1;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{return new NewTopic(topic, partitions, replication);  })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult.all().get();
            System.out.println("topics are created successfully");
        } catch (Exception e) {
            System.out.println("Exception creating topics :  "+e.getMessage());
        }
    }

    public static void createTopicsCopartitioningDemo(Properties config, List<String> alphabets, String topics ) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication = 1;

        var newTopics = alphabets
                .stream()
                .map(topic -> {
                    if (topic.equals(topics)) {
                        return new NewTopic(topic, 3, replication);
                    }
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult.all().get();
            System.out.println("topics are created successfully");
        } catch (Exception e) {
            System.out.printf("Exception creating topics : {} ", e.getMessage(), e);
        }
    }


    }
