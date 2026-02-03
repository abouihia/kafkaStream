package com.withoutspring.topology.aggregator;


import com.withoutspring.domain.Order;
import com.withoutspring.domain.Store;
import com.withoutspring.domain.TotalRevenue;
import com.withoutspring.domain.TotalRevenueWithAddress;
import com.withoutspring.serdes.SerdesFactory;
import com.withoutspring.topology.ExploreWindowTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

public  final class AggregatorUtils {


    public  static void aggergateOrderByFinalAmountByTimeWindows(KStream<String, com.withoutspring.domain.Order> generalStream,
                                                                 String storeName,
                                                                 KTable<String, com.withoutspring.domain.Store> storeStreamTable) {

        TimeWindows timeWindows  = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(15));

        Initializer<com.withoutspring.domain.TotalRevenue>   alphabetWordAggregateInitializer= com.withoutspring.domain.TotalRevenue::new;
        org.apache.kafka.streams.kstream.Aggregator<String, com.withoutspring.domain.Order, com.withoutspring.domain.TotalRevenue> alphabetWordAggregateAggregator =
                (key, order, aggregate) -> aggregate.updateTotalRevenue(key, order);


        var revenueKTable =   generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), com.withoutspring.serdes.SerdesFactory.orderSerdesGenerics()))
                .windowedBy(timeWindows)
                .aggregate(alphabetWordAggregateInitializer,
                        alphabetWordAggregateAggregator,
                        Materialized.<String, com.withoutspring.domain.TotalRevenue, WindowStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(com.withoutspring.serdes.SerdesFactory.totalRevenue()));

        revenueKTable
                .toStream()
                .peek((key, value)->{
                    System.out.println("Store Name :"+storeName+", key:"+ key.key()+", value:"+value);
                    com.withoutspring.topology.ExploreWindowTopology.printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, com.withoutspring.domain.TotalRevenue>toSysOut()
                        .withLabel(storeName+"ByStore"));

        //kTable - kTable
        ValueJoiner<com.withoutspring.domain.TotalRevenue, com.withoutspring.domain.Store, com.withoutspring.domain.TotalRevenueWithAddress> valueJoiner = com.withoutspring.domain.TotalRevenueWithAddress::new;
         var joinedParams = Joined.with(Serdes.String(), com.withoutspring.serdes.SerdesFactory.totalRevenue(), com.withoutspring.serdes.SerdesFactory.storeSerders());

        var revenuWithStoreTable =  revenueKTable
                .toStream()
                .map((key, value)-> KeyValue.pair(key.key(), value))
                .join(storeStreamTable, valueJoiner,joinedParams);

        revenuWithStoreTable
                .print(Printed.<String, com.withoutspring.domain.TotalRevenueWithAddress>toSysOut()
                        .withLabel(storeName+"ByStore"));



    }

    public static void aggergateOrderByCountByTimeWindows(KStream<String, Order> generalStream,
                                                           String generalOrdersCountWindows,
                                                           KTable<String, Store> storeStreamTable) {

        TimeWindows timeWindows  = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(15));

        var ordersCountPerStore =
                generalStream
                        .map((key, value) -> KeyValue.pair(value.locationId(), value))
                        .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesGenerics()))
                        .windowedBy(timeWindows)
                        .count(Named.as(generalOrdersCountWindows), Materialized.as(generalOrdersCountWindows))
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        ordersCountPerStore
                .toStream()
                .peek((key, value)->{
                    System.out.println("Store Name :"+generalOrdersCountWindows+", key:"+ key+", value:"+value);
                    ExploreWindowTopology.printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut()
                        .withLabel(generalOrdersCountWindows));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;


    }

    public static void aggergateOrderByCount(KStream<String, Order> generalStream, String storeName) {

        var ordersCountPerStore =
                generalStream
                        //   .map((key, value) -> KeyValue.pair(value.locationId(), value))
                        .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesGenerics()))
                        .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut()
                        .withLabel(storeName));

    }

    public  static void aggergateOrderByFinalAmount(KStream<String, Order> generalStream, String storeName,
                                                    KTable<String, Store> storeStreamTable) {

        Initializer<TotalRevenue>   alphabetWordAggregateInitializer= TotalRevenue::new;
        org.apache.kafka.streams.kstream.Aggregator<String, Order, TotalRevenue> alphabetWordAggregateAggregator =
                (key, order, aggregate) -> aggregate.updateTotalRevenue(key, order);


        var revenueKTable =   generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesGenerics()))
                .aggregate(alphabetWordAggregateInitializer,
                        alphabetWordAggregateAggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenue()));

        //kTable - kTable
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenuWithStoreTable =  revenueKTable.join(storeStreamTable, valueJoiner);

        revenuWithStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut()
                        .withLabel(storeName+"ByStore"));

    }
}
