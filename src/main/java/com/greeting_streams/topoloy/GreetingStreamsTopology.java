package com.greeting_streams.topoloy;


import com.greeting_streams.domain.GreetingRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GreetingStreamsTopology {

    public static String GREETINGS ="greetings";
    public static String GREETINGS_OUTPUT="greetings-output";

    @Autowired
    public void process(StreamsBuilder streamsBuilder){

        var greetingsStream = streamsBuilder .stream(GREETINGS,   Consumed.with(Serdes.String(), new JacksonJsonSerde<>(GreetingRecord.class))    );

        greetingsStream .print(Printed.<String , GreetingRecord>toSysOut().withLabel("greetingsStream"));

        var modifiedStream = greetingsStream    .mapValues((readOnlyKey, value) ->{
                    return    new GreetingRecord(value.message().toUpperCase(), value.timeStamp());
                });


        modifiedStream .print(Printed.<String ,GreetingRecord>toSysOut().withLabel("modifiedStream"));

        modifiedStream .to(GREETINGS_OUTPUT, Produced.with(Serdes.String(), new JacksonJsonSerde<>(GreetingRecord.class))  );

    }

}