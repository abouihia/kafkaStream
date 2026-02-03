package com.withoutspring.serdes;

import com.withoutspring.domain.GeetingRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class GreetingsDeserializer implements Deserializer<GeetingRecord> {

    private static final Logger logger  = LoggerFactory.getLogger(GreetingsDeserializer.class);

    private ObjectMapper objectMapper;

    public GreetingsDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public GeetingRecord deserialize(String s, byte[] bytes) {
        try {
            return   objectMapper.readValue(bytes, GeetingRecord.class);
        } catch (IOException e) {
            logger.info(e.getLocalizedMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.info(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }
}
