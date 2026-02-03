package com.order.controller;


import com.order.dto.AllOrdersCountPerStoreDTO;
import com.order.dto.OrdersCountPerStoreByWindowsDTO;
import com.order.services.OrderService;


import com.order.services.OrderWindowService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrderController {

    private OrderService orderService;
    private OrderWindowService orderWindowService;

    public OrderController(OrderService orderService,OrderWindowService orderWindowService) {
        this.orderWindowService = orderWindowService;
        this.orderService  = orderService;
    }

    @GetMapping("/counts")
    public List<AllOrdersCountPerStoreDTO> getAllOrder(){

        return orderService.getAllOrderCount();
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(@PathVariable("order_type") String orderType,
                                          @RequestParam(value="location_id", required = false) String locationId,
                                         @RequestParam(value="hosts_param", required = false) String hosts ){

        //     return str != null && !str.isBlank();

        if(StringUtils.hasText(hosts)){
            hosts = "true";
        }

        //str != null && !str.isEmpty();
        if(StringUtils.hasLength(locationId)){
           return ResponseEntity.ok(orderService.getOrdersCountByLocation(orderType, locationId)) ;
        }
       return  ResponseEntity.ok(orderService.getOrdersCount(orderType, hosts));
    }

    @GetMapping("/revenue/{revenue_type}")
    public ResponseEntity<?> revenueByOrder(@PathVariable("revenue_type") String orderType,
                                         @RequestParam(value="location_id", required = false) String locationId){
        if(StringUtils.hasLength(locationId)){
            return ResponseEntity.ok(orderService.getOrdersRevenueByLocation(orderType, locationId)) ;
        }
        return  ResponseEntity.ok(orderService.revenueByOrderType(orderType));
    }

/*************  Windows  ****************************/
    @GetMapping("/windows/counts")
    public List<OrdersCountPerStoreByWindowsDTO>  getAllOrderCountByWindows(){

        return  orderWindowService.getAllOrdersCountByWindows();
    }


    @GetMapping("/windows/count/{revenue_type}")
    public List<OrdersCountPerStoreByWindowsDTO>  orderCountWindow(@PathVariable("revenue_type") String orderType){
        return  orderWindowService.getOrdersCountWindowsByType(orderType);
    }


    @GetMapping("/windows/counttime")
    public List<OrdersCountPerStoreByWindowsDTO>   orderCountWindowByInterval(
                          @RequestParam(value = "from_time" )  @DateTimeFormat(iso=DateTimeFormat.ISO.DATE_TIME)LocalDateTime fromTime,
                          @RequestParam(value = "to_time") @DateTimeFormat(iso=DateTimeFormat.ISO.DATE_TIME)LocalDateTime toTime){

        return  orderWindowService.getOrdersCountWindowsbetweenTime(fromTime, toTime);
    }

    @GetMapping("/windows/revenue/{revenue_type}")
    public List<OrdersCountPerStoreByWindowsDTO>  orderRevenueWindow(@PathVariable("revenue_type") String orderType){

        return  orderWindowService.getOrdersCountWindowsByType(orderType);
    }


}
