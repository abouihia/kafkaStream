package com.withoutspring.serdes;

import com.withoutspring.domain.GeetingRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.Serializers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
@Slf4j
public class GreetingsSerializer  implements Serializer<GeetingRecord> {


    private  ObjectMapper objectMapper;

    public GreetingsSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topics, GeetingRecord geetingRecord) {
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
