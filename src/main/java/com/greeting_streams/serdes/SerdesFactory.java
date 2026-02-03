package com.greeting_streams.serdes;

import com.greeting_streams.domain.Alphabet;
import com.greeting_streams.domain.AlphabetWordAggregate;
import com.greeting_streams.domain.GreetingRecord;
import com.order.domain.Order;
import com.order.domain.Revenue;
import com.order.domain.Store;
import com.order.domain.TotalRevenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {


    static public Serde<GreetingRecord>  geetingRecordSerde(){
        return   new     GreetingSerde();
    }


    static public Serde<Order> orderSerdesGenerics(){
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(Order.class));
    }

    static public Serde<Revenue> revenueSerde(){
        return Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(Revenue.class));
    }

    static public Serde<GreetingRecord>  geetingSerdesGenerics(){

        return Serdes.serdeFrom( new JsonSerializer<>(),
                new JsonDeserializer<>(GreetingRecord.class));
    }



    public static Serde<AlphabetWordAggregate> alphabetWordAggregate() {
        return  Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(AlphabetWordAggregate.class));
    }

    public static Serde<TotalRevenue> totalRevenue() {
        return  Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(TotalRevenue.class));
    }

    public static Serde<Alphabet> alphabet() {
        return  Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(Alphabet.class));
    }


    public static Serde<Store> storeSerders() {
        return  Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(Store.class));
    }
}

