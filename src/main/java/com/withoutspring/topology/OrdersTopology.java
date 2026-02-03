package com.withoutspring.topology;

import com.withoutspring.domain.*;
import com.withoutspring.time.OrderTimeStampExtractor;
import com.withoutspring.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import static com.withoutspring.topology.aggregator.AggregatorUtils.aggergateOrderByFinalAmountByTimeWindows;



public class OrdersTopology {

    public static final String ORDERS = "orders";
    public static final String STORES = "stores";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";

    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_windows";

    public static final String RESTAURANT_ORDERS_TOTAL_REVENUR = "restaurant_orders_total_revenue";
    public static final String RESTAURANT_ORDERS_TOTAL_REVENUR_WINDOWS = "restaurant_orders_total_revenue_windows";

    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";

    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_windows";
    public static final String GENERAL_ORDERS_TOTAL_REVENUE_WINDOWS = "general_orders_total_recenue_windows";

    public static final String GENERAL_ORDERS_TOTAL_REVENUE = "general_orders_total_revenue";


    public static Topology buildToplogy() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var orderStream = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(),
                                SerdesFactory.orderSerdesGenerics()).withTimestampExtractor(new OrderTimeStampExtractor()))
                .selectKey((key,value)-> value.locationId())
                ;

        orderStream.print(
                Printed.<String, Order>toSysOut()
                        .withLabel("orders"));

       var storeStreamTable =
               streamsBuilder.table(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerders()));

        /*  creation de deux branch de notre original stream Order
         la split vie le predicat qui donne les indication de split en fonction du type d'order
        is general or restaurant pour notre example.*/
        orderStream.split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate, Branched.withConsumer(generalStream -> {
                    generalStream.print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

                    aggergateOrderByFinalAmountByTimeWindows(generalStream, RESTAURANT_ORDERS_TOTAL_REVENUR_WINDOWS, storeStreamTable);

                   /* generalStream.mapValues((key, value) ->revenueValueMapper.apply(value))
                   // generalStream.mapValues(revenueValueMapper).to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));*/
                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantStream -> {
                    restaurantStream.print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

                  /* restaurantStream.mapValues((key, value) ->revenueValueMapper.apply(value))
                   // restaurantStream.mapValues(revenueValueMapper).to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));*/
                    aggergateOrderByFinalAmountByTimeWindows(restaurantStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storeStreamTable);
                //   aggergateOrderByFinalAmount(restaurantStream, GENERAL_ORDERS_TOTAL_REVENUE, storeStreamTable);
                }));


        return streamsBuilder.build();
    }




}
