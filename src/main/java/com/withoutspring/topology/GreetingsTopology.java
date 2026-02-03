package com.withoutspring.topology;

import com.withoutspring.domain.GeetingRecord;
import com.withoutspring.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {


    public static  String GREETINGS = "greetings";
    public static  String GREETINGS_ENGILSH = "greetings_English";

    public static  String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology  buildToplogy(){
        StreamsBuilder streamsBuilder   = new StreamsBuilder();


        var  builderStream =  streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.geetingSerdesGenerics()));

        var  builderStreamEnglish =  streamsBuilder.stream(GREETINGS_ENGILSH,Consumed.with(Serdes.String(), SerdesFactory.geetingSerdesGenerics())

        );

        KStream<String, GeetingRecord> mergedStream =  builderStreamEnglish.merge(builderStream);

        mergedStream.print(Printed.<String, GeetingRecord>toSysOut().withLabel("builderStream"));

        var modifiesStream = exploreErrors(mergedStream);


        modifiesStream.print(Printed.<String, GeetingRecord>toSysOut().withLabel("modifiesStream"));

        modifiesStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.geetingSerdesGenerics()));

        modifiesStream.print(Printed.<String, GeetingRecord>toSysOut().withLabel("modifiesStream"));


          //    kafka-console-producer  --broker-list localhost:9092 --topic greetings  property "key.separator=-" --property "parse.key=true"
         //     kafka-console-producer --broker-list localhost:9092 --topic greetings_English --property "key.separator=-" --property "parse.key=true"
        //     kafka-console-producer  --broker-list localhost:9092 --topic greetings

        return streamsBuilder.build();
    }

    private static KStream<String, GeetingRecord> exploreErrors(KStream<String, GeetingRecord> mergedStream) {
        return  mergedStream.mapValues((key, value)->{
            System.out.println("message error");
            if (value.message().equals("Transient Error")) throw new IllegalStateException(value.message());
            return new GeetingRecord(value.message().toUpperCase(), value.timeStamp());
        });
    }

}
