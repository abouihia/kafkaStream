package com.greeting_streams.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

@Slf4j
public class StreamSerializationExceptionHandler implements ProductionExceptionHandler {

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {

        log.error("Serialization Exception is :{}  , and the Kafka record is : {} ",exception.getMessage(),record    );

        return ProductionExceptionHandlerResponse.CONTINUE;

    }

    @Override
    public ProductionExceptionHandlerResponse handle(ErrorHandlerContext context, ProducerRecord<byte[], byte[]> record, Exception exception) {
        return ProductionExceptionHandler.super.handle(context, record, exception);
    }

    @Override
    public ProductionExceptionHandlerResponse handleSerializationException(ProducerRecord record, Exception exception) {
        return ProductionExceptionHandler.super.handleSerializationException(record, exception);
    }

    @Override
    public ProductionExceptionHandlerResponse handleSerializationException(ErrorHandlerContext context, ProducerRecord record, Exception exception, SerializationExceptionOrigin origin) {
        return ProductionExceptionHandler.super.handleSerializationException(context, record, exception, origin);
    }
}
