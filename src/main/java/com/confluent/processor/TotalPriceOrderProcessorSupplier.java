package com.confluent.processor;

import io.confluent.developer.avro.ElectronicOrder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

public class TotalPriceOrderProcessorSupplier implements ProcessorSupplier<String, ElectronicOrder, String, Double> {


        final String storeName;

        public TotalPriceOrderProcessorSupplier(String storeName) {
                this.storeName = storeName;
        }

        @Override
        public Processor<String, ElectronicOrder, String, Double> get() {
        return new Processor<>() {
            private ProcessorContext<String, Double> context;
            private KeyValueStore<String, Double> store;

            @Override
            public void init(ProcessorContext<String, Double> context) {
                this.context = context;
                store = context.getStateStore(storeName);
                this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
            }

            private void forwardAll(final long timestamp) {
                try (KeyValueIterator<String, Double> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, Double> nextKV = iterator.next();
                        final org.apache.kafka.streams.processor.api.Record<String, Double> totalPriceRecord = new org.apache.kafka.streams.processor.api.Record<>(nextKV.key, nextKV.value, timestamp);
                        context.forward(totalPriceRecord);
                        System.out.println("Punctuation forwarded record - key " + totalPriceRecord.key() + " value " + totalPriceRecord.value());
                    }
                }
            }

            @Override
            public void process(Record<String, ElectronicOrder> record) {
                final String key = record.key();
                Double currentTotal = store.get(key);
                if (currentTotal == null) {
                    currentTotal = 0.0;
                }
                Double newTotal = record.value().getPrice() + currentTotal;
                store.put(key, newTotal);
                System.out.println("Processed incoming record - key " + key + " value " + record.value());
            }
        };
    }

        @Override
        public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(Stores.keyValueStoreBuilder(
                                                                Stores.persistentKeyValueStore(storeName),
                                                                Serdes.String(),
                                                                Serdes.Double()));
    }

}
