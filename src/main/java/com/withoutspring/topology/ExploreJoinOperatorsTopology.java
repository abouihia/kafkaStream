package com.withoutspring.topology;

import com.withoutspring.domain.Alphabet;
import com.withoutspring.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class ExploreJoinOperatorsTopology {

    public static final String ALPHABETS = "alphabets";
    public static final String ALPHABETS_ABBREVATIONS= "alphabets_abbreviations";
    public static final String ALPHBETS_STORE = "alphbets-store";
    public static final String ALPHABETS_WITH_ABBREVIATION = "alphabets-with-abbreviation";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
         //joinKStreamWithKTable(streamsBuilder);   kstream - ktable
       // joinKStreamWithGlobalKTable(streamsBuilder);  //kstream - GlobalKTable
      //  joinKTableWithKTable(streamsBuilder); //ktable - ktable
        joinKStreamWithKStream(streamsBuilder); //kstream - kstream
        return  streamsBuilder.build();
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {

        var    alphabetsAbbreviations  =  streamsBuilder.stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviations.print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        var    alphabetsStream=  streamsBuilder.stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));


        alphabetsStream.print(Printed.<String,String>toSysOut().withLabel(ALPHABETS));
        /* perform Join*/

        ValueJoiner<String, String, Alphabet>  valueJoiner= Alphabet::new;
        var  fiveSecondeWindows =  JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));
        var joinedParams  = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName("Jointure_gauche")
                .withStoreName("Jointure_gauche");

        var  joinedStream =alphabetsAbbreviations.join(alphabetsStream,
                valueJoiner,
                fiveSecondeWindows,
                joinedParams);


        joinedStream.print(Printed.<String,Alphabet>toSysOut().withLabel("alphabets_alphabets_abbreviations_kstreams"));

        System.out.println("***********************************");
    }


    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

        var    alphabetsAbbreviations  =  streamsBuilder.stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviations.print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        var    alphabetsTable =  streamsBuilder
                .globalTable(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as(ALPHBETS_STORE));



        /* perform Join*/

        ValueJoiner<String, String, Alphabet>  valueJoiner= Alphabet::new;
        KeyValueMapper<String, String,String>  KeyValueMapper  =(leftKey, rightKey) ->leftKey;

        var  joinedStream =alphabetsAbbreviations.join(alphabetsTable,KeyValueMapper, valueJoiner);

        joinedStream.print(Printed.<String,Alphabet>toSysOut().withLabel(ALPHABETS_WITH_ABBREVIATION));

    }
    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {


      var    alphabetsAbbreviations  =  streamsBuilder.stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviations.print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        var    alphabetsTable =  streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                                , Materialized.as(ALPHBETS_STORE));

        alphabetsTable.toStream().print(Printed.<String,String>toSysOut().withLabel(ALPHABETS));

        /* perform Join*/

        ValueJoiner<String, String, Alphabet>  valueJoiner= Alphabet::new;

           var  joinedStream =alphabetsAbbreviations.join(alphabetsTable, valueJoiner);

        joinedStream.print(Printed.<String,Alphabet>toSysOut().withLabel(ALPHABETS_WITH_ABBREVIATION));


    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {


        var    alphabetsAbbreviations  =  streamsBuilder.table(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviations.toStream().print(Printed.<String,String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        var    alphabetsTable =  streamsBuilder .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),Materialized.as(ALPHBETS_STORE));

        alphabetsTable.toStream().print(Printed.<String,String>toSysOut().withLabel(ALPHABETS));

        /* perform Join*/

        ValueJoiner<String, String, Alphabet>  valueJoiner= Alphabet::new;

        var  joinedStream =alphabetsAbbreviations.join(alphabetsTable, valueJoiner);

        joinedStream.toStream().print(Printed.<String,Alphabet>toSysOut().withLabel(ALPHABETS_WITH_ABBREVIATION));


    }


}
