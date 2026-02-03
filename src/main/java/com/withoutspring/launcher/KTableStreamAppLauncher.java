package com.withoutspring.launcher;

import com.withoutspring.topology.ExploreKTableTopology;
import com.withoutspring.topology.GreetingsTopology;
import com.withoutspring.utils.LauncherUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Properties;

@Slf4j
public class KTableStreamAppLauncher {

    static void main(String[] args) {



        //1- créer la configuration properties
        Properties properties  = LauncherUtils.initConfigProperties( "ktable");

        //2- créer les topics
        LauncherUtils.createTopics(properties , List.of(ExploreKTableTopology.WORDS));

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(properties,GreetingsTopology.buildToplogy() );


    }


}
