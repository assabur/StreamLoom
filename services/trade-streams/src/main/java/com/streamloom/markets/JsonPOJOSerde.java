package com.streamloom.markets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonPOJOSerde<T> implements Serde<T> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> clazz;

    public JsonPOJOSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() {}

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                return new byte[0];
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            try {
                return mapper.readValue(data, clazz);
            } catch (Exception e) {
                return null;
            }
        };
    }

    public static class Serializer<T> implements org.apache.kafka.common.serialization.Serializer<T> {
        private final ObjectMapper mapper = new ObjectMapper();
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}
        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                return new byte[0];
            }
        }
        @Override
        public void close() {}
    }

    public static class Deserializer<T> implements org.apache.kafka.common.serialization.Deserializer<T> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Class<T> clazz;
        public Deserializer(Class<T> clazz) { this.clazz = clazz; }
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}
        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return mapper.readValue(data, clazz);
            } catch (Exception e) {
                return null;
            }
        }
        @Override
        public void close() {}
    }
}
