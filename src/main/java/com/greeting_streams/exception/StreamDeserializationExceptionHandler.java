package com.greeting_streams.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class StreamDeserializationExceptionHandler implements  DeserializationExceptionHandler {

    int errorCounter = 0;

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {

       log.error("Exception is :" + exception.getMessage() + " and the kafka record is  : "+record);
       log.error("errorCounter is :"+ errorCounter);

        if( errorCounter < 2) {
            errorCounter ++;
           return DeserializationHandlerResponse.CONTINUE;
        }
      return     DeserializationHandlerResponse.FAIL;
    }


}
