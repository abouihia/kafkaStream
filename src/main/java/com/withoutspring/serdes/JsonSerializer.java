package com.withoutspring.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class JsonSerializer<T>  implements Serializer<T> {


    private final ObjectMapper objectMapper  = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


    @Override
    public byte[] serialize(String s, T t) {
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }catch (Exception e){
            System.out.println(e.getMessage());
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
