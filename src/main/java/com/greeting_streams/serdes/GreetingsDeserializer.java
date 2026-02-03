package com.greeting_streams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.greeting_streams.domain.GreetingRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class GreetingsDeserializer implements Deserializer<GreetingRecord> {

    private static final Logger logger
            = LoggerFactory.getLogger(GreetingsDeserializer.class);

    private ObjectMapper objectMapper;

    public GreetingsDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public GreetingRecord deserialize(String s, byte[] bytes) {
        try {
            return   objectMapper.readValue(bytes, GreetingRecord.class);
        } catch (IOException e) {
            logger.info(e.getLocalizedMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.info(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }
}
