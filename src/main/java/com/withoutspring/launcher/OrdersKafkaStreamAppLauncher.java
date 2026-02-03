package com.withoutspring.launcher;

import com.withoutspring.topology.OrdersTopology;
import com.withoutspring.utils.LauncherUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;


@Slf4j
public class OrdersKafkaStreamAppLauncher {



    static void main(String[] args) {

        //1- créer la configuration properties
        Properties  config =  LauncherUtils.initConfigProperties( "orders-app");

        // option1:  per Stream (recommended :  KStream stream = builder.stream("orders",Consumed.with(OrderSerdes, OrderTimeExtractor));
        // Option 2: Global (all streams)

        /*
        And then all the windows will be created based on the extracted value from this actual implementation.
         And then time windows will be created and the records will be aggregated accordingly.
         */
      //  config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimeStampExtractor.class);

        //2- créer les topics
        LauncherUtils.createTopics(config, LauncherUtils.ordersTopics);

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(config, OrdersTopology.buildToplogy());


    }









}
