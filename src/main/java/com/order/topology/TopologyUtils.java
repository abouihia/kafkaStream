package com.order.topology;

import lombok.extern.slf4j.Slf4j;
import com.order.domain.*;
import com.order.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;


@Slf4j
public class TopologyUtils {



    public static void aggregateOrdersRevenueByTimeWindows(KStream<String, Order> generalOrdersStream, String storeName,
                                                            KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        Initializer<TotalRevenue> totalRevenueInitializer =TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator =(key, value, aggregate) -> aggregate.updateRunningRevenue(key,value);

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                );

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info(" StoreName : {}, key: {} , value : {}  ", storeName, key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>,TotalRevenue>toSysOut().withLabel(storeName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinedParams =
                Joined.with(Serdes.String(),SerdesFactory.totalRevenueSerdes(), SerdesFactory.storeSerdes() );

        revenueTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storesTable, valueJoiner, joinedParams)
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));


//        revenueWithStoreTable
//                .toStream()
//                ;

    }


    public static void aggregateOrdersCountByTimeWindows
            (KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        var ordersCountPerStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded() .shutDownWhenFull()) );

        ordersCountPerStore
                .toStream()
                //.print(Printed.<String, Long>toSysOut().withLabel(storeName));
                .peek((key, value) -> {
                    log.info(" StoreName : {}, key: {} , value : {}  ", storeName, key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

//        var revenueWithStoreTable = ordersCountPerStore
//                .join(storesTable, valueJoiner);
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bystore"));
    }

    public static void aggregateOrdersCountByTimeWindowsWithGracePeriod
            (KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(60);
        Duration gracewindowSize = Duration.ofSeconds(15);
        TimeWindows timeWindowsWithGrace = TimeWindows.ofSizeAndGrace(windowSize, gracewindowSize);

        var ordersCountPerStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindowsWithGrace)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded() .shutDownWhenFull()) );

        ordersCountPerStore
                .toStream()
                //.print(Printed.<String, Long>toSysOut().withLabel(storeName));
                .peek((key, value) -> {
                    log.info(" StoreName : {}, key: {} , value : {}  ", storeName, key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

//        var revenueWithStoreTable = ordersCountPerStore
//                .join(storesTable, valueJoiner);
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bystore"));
    }




    public static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream,
                                                 String storeName,
                                                 KTable<String, Store> storesTable) {

       Initializer<TotalRevenue> totalRevenueInitializer  =TotalRevenue::new;
       var  totalRevenueKeyValueStoreMaterialized =  Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
              .withKeySerde(Serdes.String())
              .withValueSerde(SerdesFactory.totalRevenueSerdes());

       Aggregator<String, Order, TotalRevenue> aggregator
                =(key, value, aggregate) -> aggregate.updateRunningRevenue(key,value);

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(totalRevenueInitializer, aggregator,  totalRevenueKeyValueStoreMaterialized);

        revenueTable
                .toStream()
                .print(Printed.<String,TotalRevenue>toSysOut().withLabel(storeName));

        //KTable-KTable join
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenueWithStoreTable = revenueTable.join(storesTable, valueJoiner);


        revenueWithStoreTable
                .toStream()
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));





    }

    public static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream,
                                               String storeName,
                                               KTable<String, Store> storesTable) {

        var ordersCountPerStore = generalOrdersStream
                //.map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        var revenueWithStoreTable = ordersCountPerStore
                .join(storesTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bystore"));


    }


    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {}, Count : {}", startTime, endTime, value);
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
