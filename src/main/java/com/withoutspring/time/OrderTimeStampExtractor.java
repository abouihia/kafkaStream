package com.withoutspring.time;

import com.withoutspring.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class OrderTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {

        var order = (Order) consumerRecord.value();
        if( order != null && order.orderDateTime()!=null){
            var timeStamp = order.orderDateTime();
            System.out.println("time Stamp in extractor : "+timeStamp );
            
          return   convertToInstantFromCST(timeStamp);
        }

        return partitionTime;
    }

    private long convertToInstantFromCST(LocalDateTime timeStamp) {
        return  timeStamp.toInstant(ZoneOffset.ofHours(-6)).toEpochMilli();
    }

    private long convertToInstantFromUTC(LocalDateTime timeStamp) {
        return  timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
