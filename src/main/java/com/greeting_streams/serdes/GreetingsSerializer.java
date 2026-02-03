package com.greeting_streams.serdes;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greeting_streams.domain.GreetingRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class GreetingsSerializer  implements Serializer<GreetingRecord> {


    private  ObjectMapper objectMapper;

    public GreetingsSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topics, GreetingRecord geetingRecord) {
        try {
            return objectMapper.writeValueAsBytes(geetingRecord);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }catch (Exception e){
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }


}
