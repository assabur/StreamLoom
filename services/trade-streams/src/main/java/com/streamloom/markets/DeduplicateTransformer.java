package com.streamloom.markets;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class DeduplicateTransformer implements ValueTransformerSupplier<String, String> {
    private final Duration window;

    public DeduplicateTransformer(Duration window) {
        this.window = window;
    }

    @Override
    public ValueTransformer<String, String> get() {
        return new ValueTransformer<>() {
            private KeyValueStore<String, Long> store;

            @Override
            public void init(ProcessorContext context) {
                store = (KeyValueStore<String, Long>) context.getStateStore("dedup-store");
            }

            @Override
            public String transform(String value) {
                long now = System.currentTimeMillis();
                Long last = store.get(value);
                if (last != null && now-last < window.toMillis()) {
                    return null;
                }
                store.put(value, now);
                return value;
            }

            @Override
            public void close() {
            }
        };
    }
}
