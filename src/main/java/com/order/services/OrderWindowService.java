package com.order.services;

import lombok.extern.slf4j.Slf4j;
import com.order.domain.TotalRevenue;
import com.order.dto.OrdersCountPerStoreByWindowsDTO;
import com.order.dto.OrdersRevenuePerStoreByWindowsDTO;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.order.services.OrderService.mapToTypeOrder;
import static com.order.topology.OrdersTopolgy.*;

@Service
@Slf4j
public class OrderWindowService {


    private OrderStoreService orderStoreService;


    public OrderWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {

        var countWindowsIterator = getCountWindowsStore(orderType).all();

        return getOrdersCountPerStoreByWindowsDTOS(countWindowsIterator ,orderType);

    }

    private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyWindowStore<String, TotalRevenue> getOrderRevenueWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getOrdersRevenueWindowsByType(String orderType) {

        var orderRevenuWindow = getOrderRevenueWindowsStore(orderType).all();


        var spliterator = Spliterators.spliteratorUnknownSize(orderRevenuWindow, 0);
        var orderTypeEnum = mapToTypeOrder(orderType);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                                keyValue.key.key(),
                                keyValue.value,
                                orderTypeEnum,
                                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                ).collect(Collectors.toList());



    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {

        var generaLOrdersCountByWindows = getOrdersCountWindowsByType(GENERAL_ORDERS);
        var restaurantOrdersCountByWindows = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

        return Stream.of(generaLOrdersCountByWindows, restaurantOrdersCountByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsbetweenTime(LocalDateTime fromTime, LocalDateTime toTime) {


        var   countGeneralOrderBetweenTime  = getCountWindowsStore(GENERAL_ORDERS)
                .fetchAll(fromTime.toInstant(ZoneOffset.UTC),toTime.toInstant(ZoneOffset.UTC));
        var   countRestaurantOrderBetweenTime  = getCountWindowsStore(RESTAURANT_ORDERS)
                .fetchAll(fromTime.toInstant(ZoneOffset.UTC),toTime.toInstant(ZoneOffset.UTC));

        List<OrdersCountPerStoreByWindowsDTO>   restaurantOrdersCountsPerStoreInIntervalTime  =
                getOrdersCountPerStoreByWindowsDTOS(countRestaurantOrderBetweenTime, RESTAURANT_ORDERS);
        List<OrdersCountPerStoreByWindowsDTO>   generalOrdersCountsPerStoreInIntervalTime     =
                getOrdersCountPerStoreByWindowsDTOS(countGeneralOrderBetweenTime, GENERAL_ORDERS);

        return  Stream.of(restaurantOrdersCountsPerStoreInIntervalTime, generalOrdersCountsPerStoreInIntervalTime)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private static @NonNull List<OrdersCountPerStoreByWindowsDTO> getOrdersCountPerStoreByWindowsDTOS(
            KeyValueIterator<Windowed<String>, Long> countRestaurantOrderBetweenTime, String orderType) {

        var spliterator = Spliterators.spliteratorUnknownSize(countRestaurantOrderBetweenTime, 0);
        var orderTypeEnum = mapToTypeOrder(orderType);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                                keyValue.key.key(),
                                keyValue.value,
                                orderTypeEnum,
                                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                ).collect(Collectors.toList());
    }
}
