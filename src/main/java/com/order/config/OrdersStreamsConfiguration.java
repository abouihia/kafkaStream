package com.order.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import com.order.exceptionhandler.StreamsProcessorCustomErrorHandlerc;
import com.order.topology.OrdersTopolgy;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@Slf4j
public class OrdersStreamsConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Value("${server.port}")
    private  Integer port;

    /*
    in case of multi-instance
     */
    @Value("${spring.application.name}")
    private  String applicationName;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig() throws UnknownHostException {

        var streamProperties = kafkaProperties.buildStreamsProperties();

        streamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringDeserializationExceptionHandler.class);
        streamProperties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, consumerRecordRecoverer);

        //in case you multi instance
        streamProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, InetAddress.getLocalHost().getHostAddress()+":"+port);
        streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, String.format("%s%s", applicationName, port));

        return new KafkaStreamsConfiguration(streamProperties);
    }
    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer(){
        log.info("Inside streamsBuilderFactoryBeanConfigurer");
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandlerc());
        };
    }

    @Bean
    public ObjectMapper objectMapper(){
        ObjectMapper objectMapper  = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
         return  objectMapper;
    }


    public DeadLetterPublishingRecoverer recoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Exception in Deserializing the message : {} and the record is : {}", ex.getMessage(),record,  ex);
                    return new TopicPartition("recovererDLQ", record.partition());
                });
    }


    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
    };

    @Bean
    public NewTopic topicBuilder() {
        return TopicBuilder.name(OrdersTopolgy.ORDERS)
                .partitions(2)
                .replicas(1)
                .build();

    }

    @Bean
    public NewTopic storeTopicBuilder() {
        return TopicBuilder.name(OrdersTopolgy.STORES)
                .partitions(2)
                .replicas(1)
                .build();

    }
}
