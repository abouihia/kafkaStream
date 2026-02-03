package com.order.config;

import com.order.dto.HostInfoDto;
import com.order.dto.OrderCountPerStoreDTO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Configuration
public class WebClientConfiguration {


    @Bean
    public WebClient webClient(){

        return WebClient.builder().build();
    }




}
