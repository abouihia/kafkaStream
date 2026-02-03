package com.order.client;

import com.order.dto.HostInfoDto;
import com.order.dto.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class OrderServiceClient {

    private WebClient webClient;

    public OrderServiceClient(WebClient webClient) {
        this.webClient = webClient;
    }


    public List<OrderCountPerStoreDTO> retrieveOrdersCountByOrderType(HostInfoDto hostInfoDto, String  orderType) {

        var basePath = "http://" + hostInfoDto.host() + ":" + hostInfoDto.port();
        var url = UriComponentsBuilder
                .fromPath(basePath + "/v1/orders/count/{order_type}")
                .queryParam("hosts_param", "false")
                .buildAndExpand(orderType).toString();

        return webClient
                .get()
                .uri(url)
                .retrieve()
                .bodyToFlux(OrderCountPerStoreDTO.class)
                .collectList()
                .block();
    }

    public OrderCountPerStoreDTO retrieveOrdersCountByOrderTypeAndLocationId(HostInfoDto hostInfoDto,
                                                                             String orderType, String locationId) {


        var basePath = "http://"+hostInfoDto.host()+":"+hostInfoDto.port();

        var url = UriComponentsBuilder
                .fromPath(basePath)
                .path("/v1/orders/count/{order_type}")
                .queryParam("query_other_hosts", "false")
                .queryParam("location_id", locationId)
                .buildAndExpand(orderType)
                .toString();
        log.info("retrieveOrdersCountByOrderType url : {} ", url);

        return webClient
                .get()
                .uri(url)
                .retrieve()
                .bodyToMono(OrderCountPerStoreDTO.class)
                .block();

    }

}

