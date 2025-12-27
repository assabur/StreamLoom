package streamloom.eod;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {
    private final ObjectMapper mapper;
    private final Class<T> type;

    public JsonSerde(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new IllegalArgumentException("json serialize failed", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return mapper.readValue(bytes, type);
            } catch (Exception e) {
                throw new IllegalArgumentException("json deserialize failed", e);
            }
        };
    }
}
