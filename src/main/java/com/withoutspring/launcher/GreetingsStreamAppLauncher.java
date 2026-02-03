package com.withoutspring.launcher;

import com.withoutspring.exception.StreamDeserializationExceptionHandler;
import com.withoutspring.exception.StreamSerializationExceptionHandler;
import com.withoutspring.topology.GreetingsTopology;
import com.withoutspring.utils.LauncherUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

@Slf4j
public class GreetingsStreamAppLauncher {

    static void main(String[] args) {

        //1- Configurer les properties
        Properties   properties  = LauncherUtils.initConfigProperties( "orders-app");

        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        //when want to customiser  serialization and deserialization exception
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamDeserializationExceptionHandler.class);
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamSerializationExceptionHandler.class);


        //2- créer les topics
        LauncherUtils.createTopics(properties, LauncherUtils.greetingTopics);

        //3- create an instance of kafkaStreams
        LauncherUtils.createStream(properties, GreetingsTopology.buildToplogy());


    }


 /*
        Moving App 'OrbStack.app' to '/Applications/OrbStack.app'
        ==> Linking Bash Completion 'orbctl.bash' to '/opt/homebrew/etc/bash_completion.d/orbctl'
        ==> Linking Fish Completion 'orbctl.fish' to '/opt/homebrew/share/fish/vendor_completions.d/orbctl.fish'
        ==> Linking Zsh Completion '_orb' to '/opt/homebrew/share/zsh/site-functions/_orb'
        ==> Linking Zsh Completion '_orbctl' to '/opt/homebrew/share/zsh/site-functions/_orbctl'
        ==> Linking Binary 'orbctl' to '/opt/homebrew/bin/orbctl'
        ==> Linking Binary 'orb' to '/opt/homebrew/bin/orb'
         */

}
