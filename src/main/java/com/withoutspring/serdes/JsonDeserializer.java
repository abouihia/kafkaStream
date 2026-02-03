package com.withoutspring.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {


    private Class<T>  destinationClasse;

    public JsonDeserializer(Class<T> destinationClasse) {
        this.destinationClasse = destinationClasse;
    }

    private final ObjectMapper objectMapper  = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


    @Override
    public T deserialize(String s, @NonNull byte[] bytes) {

        try {
            return   objectMapper.readValue(bytes, destinationClasse);
        } catch (IOException e) {
            log.info(e.getLocalizedMessage());
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            log.info(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }
}
