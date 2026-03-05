package com.confluent.time;

import io.confluent.developer.avro.ElectronicOrder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class OrderTimestampExtractor   implements TimestampExtractor {


    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        ElectronicOrder order = (ElectronicOrder) record.value();
        System.out.println("Extracting time of " + order.getTime() + " from " + order);

        return order.getTime();
    }
}
