package com.greeting_streams.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.greeting_streams.domain.GreetingRecord;
import lombok.extern.slf4j.Slf4j;


import java.time.LocalDateTime;
import java.util.List;

import static com.greeting_streams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class GreetingMockDataProducer {

    static String GREETINGS = "greetings";

    static void main(String[] args) {
     ObjectMapper objectMapper    = new ObjectMapper()
               .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        englishGreeting(objectMapper);
     //   spanishGreeting(objectMapper);


    }

    // creation de message à publier
    private static void englishGreeting(ObjectMapper objectMapper) {
        var engGreetings = List.of(
                new GreetingRecord("Hello, Good Morning", LocalDateTime.now()),
                new GreetingRecord("Hello, Good Evening", LocalDateTime.now()),
                new GreetingRecord("Transient Error", LocalDateTime.now())
        );

        extracted(engGreetings, objectMapper, "Published English the alphabet message : {} ");
    }


    // creation de message à publier
    private static void spanishGreeting(ObjectMapper objectMapper) {
        var spanishGreeting = List.of(
                new GreetingRecord("¡Hola buenos dias!", LocalDateTime.now()),
                new GreetingRecord("¡Hola buenas tardes!", LocalDateTime.now()),
                new GreetingRecord("¡Hola, buenas noches!", LocalDateTime.now())
        );

        extracted(spanishGreeting, objectMapper, "Published  spanish the alphabet message : {} ");
    }

    private static void extracted(List<GreetingRecord> spanishGreeting, ObjectMapper objectMapper, String s) {
        spanishGreeting.forEach(
                greeting -> {
                    String greetingJSON = null;
                    try {
                        greetingJSON = objectMapper.writeValueAsString(greeting);
                        System.out.printf(greetingJSON.toString());
                        var recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                        log.info(s, recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.info("vous avez une erreur :" + e.getMessage() );
                        throw new RuntimeException(e);
                    }

                }
        );
    }
}