package com.order.services;

import com.order.dto.HostInfoDto;
import com.order.dto.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import com.order.domain.TotalRevenue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderStoreService {

    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> ordersCountStore(String storeName) {

        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(

                        storeName,
                        QueryableStoreTypes.keyValueStore()
                ));

    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> ordersRevenueStore(String storeName) {

        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(

                        storeName,
                        QueryableStoreTypes.keyValueStore()
                ));

    }


    public ReadOnlyWindowStore<String, Long> ordersWindowsCountStore(String generalOrdersCountWindows) {

         return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        generalOrdersCountWindows,
                        QueryableStoreTypes.windowStore()
                ));
    }

    public ReadOnlyWindowStore<String, TotalRevenue> ordersWindowsRevenueStore(String storeName) {

        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.windowStore()
                ));
    }


}
