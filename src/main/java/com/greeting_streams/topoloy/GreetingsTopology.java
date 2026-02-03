package com.greeting_streams.topoloy;

import com.greeting_streams.domain.GreetingRecord;
import com.greeting_streams.serdes.SerdesFactory;
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

    public static Topology buildToplogy(){
        StreamsBuilder streamsBuilder   = new StreamsBuilder();


        var  builderStream =  streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.geetingSerdesGenerics()));

        var  builderStreamEnglish =  streamsBuilder.stream(GREETINGS_ENGILSH,Consumed.with(Serdes.String(),
                SerdesFactory.geetingSerdesGenerics())

        );

        KStream<String, GreetingRecord> mergedStream =  builderStreamEnglish.merge(builderStream);

        mergedStream.print(Printed.<String, GreetingRecord>toSysOut().withLabel("builderStream"));

        var modifiesStream = exploreErrors(mergedStream);


        modifiesStream.print(Printed.<String, GreetingRecord>toSysOut().withLabel("modifiesStream"));

        modifiesStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.geetingSerdesGenerics()));

        modifiesStream.print(Printed.<String, GreetingRecord>toSysOut().withLabel("modifiesStream"));


        //    kafka-console-producer  --broker-list localhost:9092 --topic greetings  property "key.separator=-" --property "parse.key=true"
        //     kafka-console-producer --broker-list localhost:9092 --topic greetings_English --property "key.separator=-" --property "parse.key=true"
        //     kafka-console-producer  --broker-list localhost:9092 --topic greetings

        return streamsBuilder.build();
    }

    private static KStream<String, GreetingRecord> exploreErrors(KStream<String, GreetingRecord> mergedStream) {
        return  mergedStream.mapValues((key, value)->{
            System.out.println("message error");
            if (value.message().equals("Transient Error")){
                try {
                    throw new IllegalStateException(value.message());

                }catch (Exception e){
                    System.out.println("Exception during processing data:" +e.getMessage());
                    return null;
                }

            }
            return  null;
           // return new GreetingRecord(value.message().toUpperCase(), value.timeStamp());
        });
    }
}
