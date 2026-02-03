package com.order.serdes;


import com.order.domain.Order;
import com.order.domain.Store;
import com.order.domain.TotalRevenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

public class SerdesFactory{


    public static Serde<TotalRevenue> totalRevenueSerdes() {
        return  Serdes.serdeFrom(new JacksonJsonSerializer<>(),
                new JacksonJsonDeserializer<>(TotalRevenue.class));
    }

    public static Serde<Order> orderSerdes() {

        return  Serdes.serdeFrom(new JacksonJsonSerializer<>(),
                new JacksonJsonDeserializer<>(Order.class));
    }

    public static Serde<Store> storeSerdes() {
        return  Serdes.serdeFrom(new JacksonJsonSerializer<>(),
                new JacksonJsonDeserializer<>(Store.class));
    }
}
