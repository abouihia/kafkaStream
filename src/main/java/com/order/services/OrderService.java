package com.order.services;


import com.order.client.OrderServiceClient;
import com.order.dto.*;
import lombok.extern.slf4j.Slf4j;
import com.order.domain.OrderType;
import com.order.domain.TotalRevenue;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.order.topology.OrdersTopolgy.*;

@Service
@Slf4j
public class OrderService {

    private OrderStoreService orderStoreService;
    private MetaDataService metaDataService;
    private OrderServiceClient orderServiceClient;

    @Value("${server.port}")
    private  Integer port;



    public OrderService(OrderServiceClient orderServiceClient,
                        MetaDataService metaDataService,
                        OrderStoreService orderStoreService) {

        this.orderServiceClient = orderServiceClient;
        this.metaDataService = metaDataService;
        this.orderStoreService = orderStoreService;
    }





    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType, String hosts) {

        var ordersCountStore = getOrderStore(orderType);
        var orders = ordersCountStore.all();
        var  spliteratorUnknown  =  Spliterators.spliteratorUnknownSize(orders, 0);
       var  orderCountPerStoreDTOSFromCurrentHost = StreamSupport.stream(spliteratorUnknown, false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

        //1. fetch the metadata about other instances
        //and  make the restcall to get the data from other instance
        // make sure the other instance is not going to make any network calls to other instances
        var    orderCountPerStoreDTOFromOtherHosts  = retriveDataFromOtherInstances( orderType, Boolean.parseBoolean(hosts));


      //2. aggregate the data.


        return Stream.of(orderCountPerStoreDTOFromOtherHosts,
                orderCountPerStoreDTOSFromCurrentHost).filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<OrderCountPerStoreDTO>  retriveDataFromOtherInstances(String orderType, boolean isQueryWithHosts)  {
        List<HostInfoDto> otherHosts =  otherHosts();
        log.info("Otherhosts : {}" , otherHosts);

        //2. make the restcall to get the data form other instance
        // make sur other instances is not going to make any network calls to other instances
        if(isQueryWithHosts &&  otherHosts!= null && !otherHosts.isEmpty()){
           return otherHosts.stream().map(el ->
                    orderServiceClient.retrieveOrdersCountByOrderType(el , orderType))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return  null;
    }

    private List<HostInfoDto> otherHosts() {
        try {
            String  currentHostAddress   = InetAddress.getLocalHost().getHostAddress();
            return   metaDataService
                    .getStreamMetaData()
                    .stream()
                    .filter( hostInfoDto-> !currentHostAddress.equals(hostInfoDto.host()) && hostInfoDto.port() != port)
                    .collect(Collectors.toList());
        } catch (UnknownHostException e) {
            log.error("Exception in OtherHosts : {}" , e.getMessage(), e);
        }
      return Collections.EMPTY_LIST;
    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getOrderRevenuStore(String revenueType) {

        return switch (revenueType) {
            case GENERAL_ORDERS -> orderStoreService.ordersRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public OrderRevenueDTO getOrdersRevenueByLocation(String revenuType, String locationId){

        var orderRevenue = getOrderRevenuStore(revenuType).get(locationId);
        if( orderRevenue != null){
            return  new OrderRevenueDTO(locationId, mapToTypeOrder(revenuType),
                    new TotalRevenue(locationId, orderRevenue.runnuingOrderCount(), orderRevenue.runningRevenue()));
        }
        return  null;
    }

    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {

        var ordersCountStore = getOrderRevenuStore(orderType);
        var orders = ordersCountStore.all();


        var  spliteratorUnknown  =  Spliterators.spliteratorUnknownSize(orders, 0);
        return  StreamSupport.stream(spliteratorUnknown, false)
                .map(keyValue ->
                        new OrderRevenueDTO(keyValue.key,mapToTypeOrder(orderType), keyValue.value))
                .collect(Collectors.toList());


    }

    public static OrderType mapToTypeOrder(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private String mapOrderCountStoreName(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL_ORDERS_COUNT;
            case RESTAURANT_ORDERS -> RESTAURANT_ORDERS_COUNT;
            default -> throw new IllegalStateException("Not a valid option");
        };


    }

    public OrderCountPerStoreDTO getOrdersCountByLocation(String orderType, String locationId) {


        String  storeName =  mapOrderCountStoreName( orderType);

        HostInfoDTOWithKey hostInfoDTOWithKey =
                     metaDataService.getStreamMetaData(storeName, locationId);

        log.info("hostInfoDTOWithKey :{}" +  hostInfoDTOWithKey);

        if( hostInfoDTOWithKey != null){
             if( hostInfoDTOWithKey.port() == port){
                 log.info("fetch the data from the current instance" );
                 var ordersCount = getOrderStore(orderType).get(locationId);
                 return  ordersCount != null ?  new OrderCountPerStoreDTO(locationId, ordersCount) : null;
             }else{
                 log.info("fetch the data from the remmote instance" );
                   //get the data form remote instance  with rest client
                 HostInfoDto  hostInfoDto =     new HostInfoDto(hostInfoDTOWithKey.host(), hostInfoDTOWithKey.port());
                return  orderServiceClient.retrieveOrdersCountByOrderTypeAndLocationId(hostInfoDto, orderType,  locationId);
             }
        }
    return  null;
    }



    public List<AllOrdersCountPerStoreDTO> getAllOrderCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> countPerStoreDTOOrderTypeBiFunction=
        (orderCountPerStoreDTO, orderType) ->
                new AllOrdersCountPerStoreDTO(orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType);

       var generalOrder   = extracted(GENERAL_ORDERS, countPerStoreDTOOrderTypeBiFunction, OrderType.GENERAL, "true");

       var restaurantOrder = extracted(RESTAURANT_ORDERS, countPerStoreDTOOrderTypeBiFunction, OrderType.RESTAURANT, "true");


        return Stream
                .of(generalOrder, restaurantOrder)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<AllOrdersCountPerStoreDTO>   extracted(String restaurantOrdersCount,
                                                        BiFunction<OrderCountPerStoreDTO, OrderType,
                                                                AllOrdersCountPerStoreDTO> countPerStoreDTOOrderTypeBiFunction,
                                                        OrderType restaurant, String hosts) {
         return getOrdersCount(restaurantOrdersCount, hosts)
                .stream()
                .map(orderCountPerStoreDTO -> countPerStoreDTOOrderTypeBiFunction
                        .apply(orderCountPerStoreDTO, restaurant)).collect(Collectors.toList());

    }


}
