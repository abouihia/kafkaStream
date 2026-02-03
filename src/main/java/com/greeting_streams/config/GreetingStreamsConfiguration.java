package com.greeting_streams.config;


import com.greeting_streams.exception.StreamProcessorCustomErrorHandler;
import com.greeting_streams.topoloy.GreetingStreamsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

@Slf4j
@Configuration
public class GreetingStreamsConfiguration {


    @Autowired
    KafkaProperties  kafkaProperties;

    @Bean
    public StreamsBuilderFactoryBeanConfigurer  streamsBuilderFactoryBeanConfigurer(){

          return  factoryBean ->{
              factoryBean.setStreamsUncaughtExceptionHandler(new StreamProcessorCustomErrorHandler());
          };
    }


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {

     var  props =  kafkaProperties.buildStreamsProperties();
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,  RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public ConsumerRecordRecoverer recoverer() {
        return
                (consumerRecord, e ) -> {
            log.error("Exception is  :{}, Failed Record :{} " , e.getMessage(),consumerRecord);
                    };
    }



    @Bean
    public NewTopic greetingInputTopics(){
        return TopicBuilder.name(GreetingStreamsTopology.GREETINGS)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic greetingOutTopics(){
        return TopicBuilder.name(GreetingStreamsTopology.GREETINGS_OUTPUT)
                .partitions(2)
                .replicas(1)
                .build();
    }
}
