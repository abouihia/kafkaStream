package com.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.dto.OrderRevenueDTO;
import com.order.dto.OrdersCountPerStoreByWindowsDTO;
import com.order.services.OrderService;
import com.order.services.OrderWindowService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import static org.hamcrest.Matchers.equalTo;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.order.topology.OrdersTopolgy.*;


@SpringBootTest
@EmbeddedKafka(topics = {ORDERS,STORES })//spin up thoses topics
@TestPropertySource(properties = {
          "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
          "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OrdersIntegrationTest {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
     OrderService  orderService;

    @Autowired
    OrderWindowService  orderWindowService;

    @BeforeEach
    void setUp() {
        streamsBuilderFactoryBean.start();;
    }

    @AfterEach
    void tearDown() {
        streamsBuilderFactoryBean.getKafkaStreams().close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }

    private  void  publishOrders(){

        OrderUtilTest.orders().forEach(order ->{
            String orderJSON = null;
            try {
                orderJSON  = objectMapper.writeValueAsString(order.value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            kafkaTemplate.send(ORDERS, order.key, orderJSON);
        });

    }


    @Test
    void orderCount() {
        //given
        publishOrders();
        //when


        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCount(GENERAL_ORDERS, "false").size(), equalTo(1) );


        //then
        var generalOrderCount =  orderService.getOrdersCount(GENERAL_ORDERS, "false");
        Assertions.assertEquals(300, generalOrderCount.get(0).orderCount());
    }



    @Test
    void orderRevenue() {
        //given
        publishOrders();
        //when


        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderType(GENERAL_ORDERS).size(), equalTo(1) );

        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderType(GENERAL_ORDERS_REVENUE).size(), equalTo(1) );


        //then
        List<OrderRevenueDTO>  generalOrdersRevenue =  orderService.revenueByOrderType(GENERAL_ORDERS);
        Assertions.assertEquals(new BigDecimal("27.00"), generalOrdersRevenue.get(0).totalRevenue().runningRevenue());
    }
    @Test
    void orderRevenue_multipleorderByWindows() {
        //given
        publishOrders();
        publishOrders();
        //when


        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderWindowService.getOrdersCountWindowsByType(GENERAL_ORDERS).size(), equalTo(1) );

        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderWindowService.getOrdersCountWindowsByType(RESTAURANT_ORDERS).size(), equalTo(1) );

        var expectedStartTime =   LocalDateTime.parse("2023-02-22T03:25:00");
        var expectedEndTime =   LocalDateTime.parse("2023-02-22T03:25:15");
        //then
        List<OrdersCountPerStoreByWindowsDTO>  generalOrdersCountWindow =  orderWindowService.getOrdersCountWindowsByType(GENERAL_ORDERS);

        System.out.println("generalOrdersCountWindow :" +generalOrdersCountWindow.get(0).startWindow());
        Assertions.assertEquals(expectedStartTime,generalOrdersCountWindow.get(0).startWindow());
        Assertions.assertEquals(expectedEndTime,generalOrdersCountWindow.get(0).endWindow());
    }



}
