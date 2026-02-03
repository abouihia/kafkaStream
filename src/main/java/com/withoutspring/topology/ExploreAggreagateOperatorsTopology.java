package com.withoutspring.topology;

import com.withoutspring.domain.AlphabetWordAggregate;
import com.withoutspring.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class ExploreAggreagateOperatorsTopology {

    public static final String AGGEGATE = "aggregate";


    public static Topology build() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var inputStream = streamsBuilder.stream(AGGEGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream.print(Printed.<String, String>toSysOut().withLabel(AGGEGATE));

        var groupedString = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        //     .groupBy((key, value)-> value, Grouped.with(Serdes.String(), Serdes.String()));

        exploreCount(groupedString);
        exploreReduce(groupedString);
        //  exploreAggregator(groupedString);
        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> groupedString) {
        var countByAlphabet = groupedString.count(Named.as("count-per-alphbet"),
                Materialized.as("count-per-alphbet"));

        countByAlphabet.toStream().print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedString) {

        var reducedValue = groupedString
                .reduce((value1, value2) -> {
                    System.out.println(value1 + " - " + value2);
                    return value1.toUpperCase() + ":::" + value2.toUpperCase();
                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduced-words")
                        .withValueSerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        reducedValue.toStream().print(Printed.<String, String>toSysOut().withLabel("words-count-per-alphabet"));
    }

    private static void exploreAggregator(KGroupedStream<String, String> groupedString) {

        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer = AlphabetWordAggregate::new;
        Aggregator<String, String, AlphabetWordAggregate> alphabetWordAggregateAggregator =
                (key, value, aggregate) -> aggregate.mapToAlphabetWordAggegator(key, value);
        var aggergatorStream = groupedString.aggregate(alphabetWordAggregateInitializer,
                alphabetWordAggregateAggregator,
                Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as("aggregated-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesFactory.alphabetWordAggregate())
        );

        aggergatorStream
                .toStream()
                .print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("aggregated-words"));
    }
}


