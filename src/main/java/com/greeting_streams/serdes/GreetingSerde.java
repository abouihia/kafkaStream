package com.greeting_streams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.greeting_streams.domain.GreetingRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class GreetingSerde  implements Serde {

    private final ObjectMapper objectMapper  = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @Override
    public Serializer<GreetingRecord> serializer() {
        return new GreetingsSerializer(objectMapper);
    }

    @Override
    public Deserializer<GreetingRecord> deserializer() {
        return new GreetingsDeserializer(objectMapper);
    }
}
