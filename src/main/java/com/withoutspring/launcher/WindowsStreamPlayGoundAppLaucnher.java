package com.withoutspring.launcher;

import com.withoutspring.topology.ExploreWindowTopology;
import com.withoutspring.utils.LauncherUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Properties;

import static com.withoutspring.topology.ExploreWindowTopology.WINDOW_WORDS;

@Slf4j
public class WindowsStreamPlayGoundAppLaucnher {

    static void main(String[] args) {

        //1- créer la configuration properties
        Properties config  = LauncherUtils.initConfigProperties("window-2");

        //2- créer les topics  
        LauncherUtils.createTopics(config, List.of(WINDOW_WORDS));
        
        try {
            //3- créer le stream Kafka  avec son topology
            LauncherUtils.createStream(config, ExploreWindowTopology.build());
        } catch (Exception e) {
            log.error("Exceptions in starting the streams :{}", e.getMessage(), e);
        }
    }




}
