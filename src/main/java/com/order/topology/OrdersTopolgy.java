package com.order.topology;

import lombok.extern.slf4j.Slf4j;
import com.order.domain.Order;
import com.order.domain.OrderType;
import com.order.domain.Revenue;
import com.order.domain.Store;
import com.order.extractor.OrderTimeStampExtractor;
import com.order.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.order.topology.TopologyUtils.*;


@Component
@Slf4j
public class OrdersTopolgy {

    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";
    public static final String STORES = "stores";


    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());


        var ordersStream = streamsBuilder
                .stream(ORDERS,  Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()).withTimestampExtractor(new OrderTimeStampExtractor()))
                .selectKey((key, value) -> value.locationId());

        ordersStream   .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        //KStream-KTable

        var storesTable = streamsBuilder.table(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerdes()));

        storesTable .toStream().print(Printed.<String,Store>toSysOut().withLabel("stores"));


        ordersStream
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {

                            generalOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

//                            generalOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(GENERAL_ORDERS,
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                            aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindowsWithGracePeriod(generalOrdersStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByTimeWindows(generalOrdersStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);

                        })
                )
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

//                            restaurantOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS,
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));

                            aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindowsWithGracePeriod(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE,storesTable);
                            aggregateOrdersRevenueByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);
                        })
                );



    }



}
