package com.withoutspring.launcher;

import com.withoutspring.topology.ExploreAggreagateOperatorsTopology;
import com.withoutspring.utils.LauncherUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
@Slf4j
public class AggreagationStreamPlayGroundAppLauncher {

    static void main(String[] args) {


        //1- Configurer les properties
        Properties config  = LauncherUtils.initConfigProperties("aggregate");
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        //2- créer les topics
        LauncherUtils.createTopics(config, List.of(ExploreAggreagateOperatorsTopology.AGGEGATE));

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(config,ExploreAggreagateOperatorsTopology.build());

    }
}
