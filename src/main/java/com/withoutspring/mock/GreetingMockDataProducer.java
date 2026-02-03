package com.withoutspring.mock;


import com.withoutspring.domain.Greeting;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.withoutspring.utils.ProducerUtil;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;




@Slf4j
public class GreetingMockDataProducer {

    static String GREETINGS = "greetings";

    static void main(String[] args) {
        ObjectMapper  objectMapper    = new ObjectMapper().registerModule(new JavaTimeModule())
                                                            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        
        englishGreeting(objectMapper);
        spanishGreeting(objectMapper);


    }


    // creation de message à publier 
   private static void englishGreeting(ObjectMapper objectMapper){
       var engGreetings = List.of(
               new Greeting("Hello, Good Morning", LocalDateTime.now()),
               new Greeting("Hello, Good Evening", LocalDateTime.now()),
               new Greeting("Transient Error", LocalDateTime.now())
       );

       publishMessages(engGreetings, objectMapper, "Published English the alphabet message : {} ");
   }


    // creation de message à publier
    private static void spanishGreeting(ObjectMapper objectMapper){
        var spanishGreeting = List.of(
                new Greeting("Hola buenos dias!", LocalDateTime.now()),
                new Greeting("Hola buenas tardes!", LocalDateTime.now()),
                new Greeting("Hola, buenas noches!", LocalDateTime.now())
        );

        publishMessages(spanishGreeting, objectMapper, "Published  spanish the alphabet message : {} ");
    }

    private static void publishMessages(List<Greeting> spanishGreeting, ObjectMapper objectMapper, String s) {
        spanishGreeting.forEach(
                greeting -> {
                    try {
                        var greetingJSON = objectMapper.writeValueAsString(greeting);
                        System.out.printf(greetingJSON.toString());
                        var recordMetaData = ProducerUtil.publishMessageSync(GREETINGS, null, greetingJSON);
                        log.info(s, recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }
}
