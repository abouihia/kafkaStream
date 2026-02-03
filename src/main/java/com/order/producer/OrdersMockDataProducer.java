package com.order.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.greeting_streams.topoloy.OrdersTopologyWithoutSpring;
import lombok.extern.slf4j.Slf4j;
import com.order.domain.Order;
import com.order.domain.OrderLineItem;
import com.order.domain.OrderType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;


import static com.greeting_streams.producer.ProducerUtil.publishMessageSync;
import static java.lang.Thread.sleep;


@Slf4j
public class OrdersMockDataProducer {

    static String ORDERS = "orders";

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        publishOrders(objectMapper, buildOrders());
        publishBulkOrders(objectMapper);


        /**
         * To test grace period.
         * 1. Run the {@link #publishOrders(ObjectMapper, List)} function during the start of the minute.
         * 2. Wait until the next minute and run the {@link #publishOrders(ObjectMapper, List)}
         *      and then the {@link #publishOrdersToTestGrace(ObjectMapper, List)} function before the 15th second.
         *      - This should allow the aggregation to be added to the window before
         *
         */
        publishOrdersToTestGrace(objectMapper, buildOrdersToTestGrace());





        System.out.println(LocalDateTime.now(ZoneId.of("UTC")));
        System.out.println(LocalDateTime.now());

        var timeStamp = LocalDateTime.now();

        System.out.println("timeStamp : " + timeStamp);

        var instant = timeStamp.toEpochSecond(ZoneOffset.ofHours(-6));
        System.out.println("instant : " + instant);



    }



    private static List<Order> buildOrders() {
        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()

        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.now()

        );

        var order3 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()

        );

        var order4 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.RESTAURANT,
                orderItems,
                LocalDateTime.now()

        );

        return List.of(order1,order2,order3,order4 );
    }

    private static void publishBulkOrders(ObjectMapper objectMapper) throws InterruptedException {

        int count = 0;
        while (count < 100) {
            var orders = buildOrders();
            publishOrders(objectMapper, orders);
            sleep(1000);
            count++;
        }
    }

    private static void publishOrders(ObjectMapper objectMapper, List<Order> orders) {

        orders
                .forEach(order -> {
                    try {
                        var ordersJSON = objectMapper.writeValueAsString(order);
                        var recordMetaData = publishMessageSync(ORDERS, order.orderId() + "", ordersJSON);
                        log.info("Published the order message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });
    }






    private static void publishOrdersToTestGrace(ObjectMapper objectMapper, List<Order> orders) {

        orders
                .forEach(order -> {
                    try {
                        var ordersJSON = objectMapper.writeValueAsString(order);
                        var recordMetaData = publishMessageSync(OrdersTopologyWithoutSpring.ORDERS, order.orderId() + "", ordersJSON);
                        log.info("Published the order message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });
    }

    private static List<Order> buildOrdersToTestGrace() {
        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse("2023-02-27T08:45:58")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.parse("2023-02-27T08:45:58")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        var order3 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-27T08:45:58")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        var order4 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.RESTAURANT,
                orderItems,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-27T08:45:58")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        return List.of( order1, order2, order3, order4 );
    }


}
