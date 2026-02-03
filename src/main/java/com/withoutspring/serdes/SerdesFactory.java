package com.withoutspring.serdes;


import com.withoutspring.domain.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {


    static public Serde<GeetingRecord>  geetingRecordSerde(){
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

    static public Serde<GeetingRecord>  geetingSerdesGenerics(){

        return Serdes.serdeFrom( new JsonSerializer<>(),
                new JsonDeserializer<>(GeetingRecord.class));
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

