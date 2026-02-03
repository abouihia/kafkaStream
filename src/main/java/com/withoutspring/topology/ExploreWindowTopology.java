package com.withoutspring.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var wordsStream = streamsBuilder.stream(WINDOW_WORDS, Consumed.with(Serdes.String(), Serdes.String()));
        //  tumblingWindows(wordsStream);
        //  hoppingWindows(wordsStream);
        slidingWindows(wordsStream);
        return streamsBuilder.build();
    }

    private static void tumblingWindows(KStream<String, String> wordsStream) {

        Duration duration = Duration.ofSeconds(5);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(duration);
        var windowingAggregation = wordsStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        windowingAggregation.toStream()
                .peek((key, value) ->
                {
                    System.out.println("tumblingWindow : key : " + key.key() + ", value : " + value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("TumblingWindow"));


    }

    private static void hoppingWindows(KStream<String, String> wordsStream) {

        Duration windowSize = Duration.ofSeconds(5);
        Duration advanceBySize = Duration.ofSeconds(3);

        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceBySize);

        var windowingAggregation = wordsStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        windowingAggregation.toStream()
                .peek((key, value) ->
                {
                    System.out.println("HoppingWindows : key : " + key.key() + ", value : " + value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("HoppingWindows"));


    }

    private static void slidingWindows(KStream<String, String> wordsStream) {


        SlidingWindows slidingWindows = SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        var windowingAggregation = wordsStream
                .groupByKey()
                .windowedBy(slidingWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        windowingAggregation.toStream()
                .peek((key, value) ->
                {
                    System.out.println("slidingWindows : key : " + key.key() + ", value : " + value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("slidingWindows"));


    }

    public static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        System.out.println("startLDT :" + startLDT + " , endLDT :" + endLDT + ", Count : " + value);
    }
}
