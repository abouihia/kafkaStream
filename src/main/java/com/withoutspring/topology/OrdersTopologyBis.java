package com.withoutspring.topology;

import com.withoutspring.domain.*;
import com.withoutspring.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Topologie Kafka Streams pour le traitement des commandes.
 * Sépare les commandes générales et restaurant, calcule les revenus agrégés
 * et enrichit les données avec les informations des magasins.
 */
public class OrdersTopologyBis {

    // Topics Kafka
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    // Topics de sortie
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String GENERAL_ORDERS = "general_orders";

    // State stores
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_TOTAL_REVENUE = "restaurant_orders_total_revenue";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_TOTAL_REVENUE = "general_orders_total_revenue";

    public static Topology buildTopology() {

        // Prédicats pour séparer les types de commandes
        Predicate<String, Order> generalPredicate = (key, order) ->
                order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) ->
                order.orderType().equals(OrderType.RESTAURANT);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Stream principal des commandes avec locationId comme clé
        var orderStream = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), SerdesFactory.orderSerdesGenerics()))
                .selectKey((key, value) -> value.locationId());

        orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));

        // Table des magasins pour enrichissement
        var storeTable = streamsBuilder.table(
                STORES,
                Consumed.with(Serdes.String(), SerdesFactory.storeSerders())
        );

        // Séparation du stream en deux branches : général et restaurant
        orderStream
                .split(Named.as("general-restaurant-stream"))
                .branch(generalPredicate, Branched.withConsumer(generalStream -> {
                    generalStream.print(
                            Printed.<String, Order>toSysOut().withLabel("generalStream")
                    );

                    aggregateOrdersByCount(generalStream, GENERAL_ORDERS_COUNT);
                    aggregateOrdersByRevenue(
                            generalStream,
                            GENERAL_ORDERS_TOTAL_REVENUE,
                            storeTable
                    );
                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantStream -> {
                    restaurantStream.print(
                            Printed.<String, Order>toSysOut().withLabel("restaurantStream")
                    );

                    aggregateOrdersByCount(restaurantStream, RESTAURANT_ORDERS_COUNT);
                    aggregateOrdersByRevenue(
                            restaurantStream,
                            RESTAURANT_ORDERS_TOTAL_REVENUE,
                            storeTable
                    );
                }));

        return streamsBuilder.build();
    }

    /**
     * Agrège le nombre de commandes par magasin
     */
    private static void aggregateOrdersByCount(KStream<String, Order> orderStream, String storeName) {
        var ordersCountPerStore = orderStream
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesGenerics()))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));
    }

    /**
     * Agrège le revenu total par magasin et enrichit avec les informations du magasin
     */
    private static void aggregateOrdersByRevenue(
            KStream<String, Order> orderStream,
            String storeName,
            KTable<String, Store> storeTable) {

        // Initializer et Aggregator pour calculer le revenu total
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator =
                (key, order, aggregate) -> aggregate.updateTotalRevenue(key, order);

        // Agrégation des revenus par locationId
        var revenueTable = orderStream
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesGenerics()))
                .aggregate(
                        totalRevenueInitializer,
                        totalRevenueAggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenue())
                );

        // Jointure KTable-KTable pour enrichir avec les données du magasin
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner =
                TotalRevenueWithAddress::new;

        var revenueWithStoreTable = revenueTable.join(storeTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut()
                        .withLabel(storeName + "-by-store"));
    }
}