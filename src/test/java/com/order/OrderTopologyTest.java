package com.order;

import com.order.domain.Order;
import com.order.domain.TotalRevenue;
import com.order.topology.OrdersTopolgy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.order.OrderUtilTest.orders;
import static com.order.topology.OrdersTopolgy.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderTopologyTest {

    TopologyTestDriver topologyTestDriver  = null;

    TestInputTopic<String, Order>  orderInputTopic  = null;

    static String INPUT_TOPIC = OrdersTopolgy.ORDERS;

    OrdersTopolgy ordersTopolgy = new OrdersTopolgy();
    StreamsBuilder streamsBuilder =  null;

    @BeforeEach
    public void setUp(){
        streamsBuilder = new StreamsBuilder();
        ordersTopolgy.process(streamsBuilder);

        topologyTestDriver  = new TopologyTestDriver(streamsBuilder.build());
        orderInputTopic  = topologyTestDriver.createInputTopic(INPUT_TOPIC,
                Serdes.String().serializer(),  new JacksonJsonSerializer<>()
        );

    }
    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }


    @Test
    void ordersCount() {
        //given
        // publishing the data in the topic
        orderInputTopic.pipeKeyValueList(orders());

        //when
        ReadOnlyKeyValueStore<String, Long> generalOrdersCountStore =
                topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_COUNT);

        var generalOrdersCount = generalOrdersCountStore.get("store_1234");
        assertEquals(1, generalOrdersCount);

        ReadOnlyKeyValueStore<String, Long> restaurantOrdersCountStore =
                topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_COUNT);

        var restaurantOrdersCount = restaurantOrdersCountStore.get("store_1234");
        assertEquals(1, restaurantOrdersCount);

    }

    @Test
    void ordersRevenu() {
        //given
        // publishing the data in the topic
        orderInputTopic.pipeKeyValueList(orders());

        //when
        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersCountStore =
                topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);

        var generalOrdersRevenueData = generalOrdersCountStore.get("store_1234");
        assertEquals(1, generalOrdersRevenueData.runnuingOrderCount());
        assertEquals(new BigDecimal("27.00"), generalOrdersRevenueData.runningRevenue());

        ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersCountStore =
                topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);

        var restaurantOrdersRevenueData = restaurantOrdersCountStore.get("store_1234");
        assertEquals(1, restaurantOrdersRevenueData.runnuingOrderCount());
        assertEquals( new BigDecimal("15.00"), restaurantOrdersRevenueData.runningRevenue());

    }


    @Test
    void ordersRevenu_multipleOrdersPerStore() {
        //given
        // publishing the data in the topic
        orderInputTopic.pipeKeyValueList(orders());
        orderInputTopic.pipeKeyValueList(orders());

        //when
        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersCountStore =
                topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);

        var generalOrdersRevenueData = generalOrdersCountStore.get("store_1234");
        assertEquals(2, generalOrdersRevenueData.runnuingOrderCount());
        assertEquals(new BigDecimal("54.00"), generalOrdersRevenueData.runningRevenue());

        ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersCountStore =
                topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);

        var restaurantOrdersRevenueData = restaurantOrdersCountStore.get("store_1234");
        assertEquals(2, restaurantOrdersRevenueData.runnuingOrderCount());
        assertEquals( new BigDecimal("30.00"), restaurantOrdersRevenueData.runningRevenue());

    }

    @Test
    void ordersRevenu_byWindows() {
        //given
        // publishing the data in the topic
        orderInputTopic.pipeKeyValueList(orders());
        orderInputTopic.pipeKeyValueList(orders());

        //when
        WindowStore<String, TotalRevenue> generalOrdersCountStoreWindow =
                topologyTestDriver.getWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);

        generalOrdersCountStoreWindow.all()
                .forEachRemaining(windowedTotalRevenueKeyValue -> {
                  var generalOrderRevenuWindow =  windowedTotalRevenueKeyValue.value;
                  var startTime = windowedTotalRevenueKeyValue.key.window().startTime();
                  var endTime = windowedTotalRevenueKeyValue.key.window().endTime();

                 var expectedStartTime =   LocalDateTime.parse("2023-02-21T21:25:00");
                 var expectedEndTime =   LocalDateTime.parse("2023-02-21T21:25:15");


                assertEquals( LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))) ,expectedStartTime );
                assertEquals(LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))) , expectedEndTime );

                assertEquals( new BigDecimal("54.00"), generalOrderRevenuWindow.runningRevenue());

                });

        WindowStore<String, TotalRevenue> restaurantOrdersCountStoreWindow =
                topologyTestDriver.getWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);

        restaurantOrdersCountStoreWindow.all()
                .forEachRemaining(windowedTotalRevenueKeyValue -> {
                    var restaurantOrderRevenue =  windowedTotalRevenueKeyValue.value;

                    var startTime = windowedTotalRevenueKeyValue.key.window().startTime();
                    var endTime = windowedTotalRevenueKeyValue.key.window().endTime();

                    var expectedStartTime =   LocalDateTime.parse("2023-02-21T21:25:00");
                    var expectedEndTime =   LocalDateTime.parse("2023-02-21T21:25:15");


                    assertEquals( LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))) ,expectedStartTime );
                    assertEquals(LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))) , expectedEndTime );


                    assertEquals(2, restaurantOrderRevenue.runnuingOrderCount());
                    assertEquals( new BigDecimal("30.00"), restaurantOrderRevenue.runningRevenue());

                });

    }





}
