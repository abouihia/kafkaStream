package com.withoutspring.launcher;

import com.withoutspring.topology.GreetingsTopology;
import com.withoutspring.utils.LauncherUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Properties;

import static com.withoutspring.topology.ExploreJoinOperatorsTopology.ALPHABETS;
import static com.withoutspring.topology.ExploreJoinOperatorsTopology.ALPHABETS_ABBREVATIONS;

@Slf4j
public class JoiningStreamPlayGroundAppLauncher {

    public  static void main(String[] args) {

        //1- Configurer les properties
        Properties config  = LauncherUtils.initConfigProperties("joins1");

        //2- créer les topics
        LauncherUtils.createTopicsCopartitioningDemo(    config, List.of(ALPHABETS, ALPHABETS_ABBREVATIONS), ALPHABETS_ABBREVATIONS);

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(config, GreetingsTopology.buildToplogy());


    }
}
