package com.withoutspring.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.withoutspring.domain.Address;
import com.withoutspring.domain.Store;
import com.withoutspring.topology.OrdersTopology;
import com.withoutspring.utils.ProducerUtil;

import java.util.List;

public class StoresMockDataProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


        var address1 = new Address("1234 Street 1 ", "", "City1", "State1", "12345");
        var store1 = new Store("store_1234", address1,"1234567890");

        var address2 = new Address("1234 Street 2 ",
                                    "", "City2",
                                    "State2", "541321");
        var store2 = new Store("store_4567", address2,"0987654321");


        var stores = List.of(store1, store2);
        stores
                .forEach(store -> {
                    try {
                        var storeJSON = objectMapper.writeValueAsString(store);
                        var recordMetaData = ProducerUtil.publishMessageSync(OrdersTopology.STORES, store.locationId(), storeJSON);
                        System.out.printf("Published the store message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        System.out.printf("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                    catch (Exception e) {
                        System.out.printf("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });

    }
}
