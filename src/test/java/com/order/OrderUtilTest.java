package com.order;

import com.order.domain.Order;
import com.order.domain.OrderLineItem;
import com.order.domain.OrderType;
import org.apache.kafka.streams.KeyValue;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public class OrderUtilTest {

    public   static List<KeyValue<String, Order>> orders(){

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
                // LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );
        var keyValue1 = KeyValue.pair( order1.orderId().toString(), order1);

        var keyValue2 = KeyValue.pair( order2.orderId().toString(), order2);


        return  List.of(keyValue1, keyValue2);

    }
}
