package com.streamloom.markets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class TradeStreamsApp {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "trade-streams"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        String rawTopic = env("RAW_TOPIC", "raw_trades");
        String enrichedTopic = env("ENRICHED_TOPIC", "enriched_trades");
        String candleTopic = env("CANDLE_TOPIC", "candles_1m");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> raw = builder.stream(rawTopic);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("dedup-store");
        builder.addStateStore(Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Long()));

        KStream<String, String> enriched = raw
                .mapValues(TradeStreamsApp::normalize)
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> keyFrom(v))
                .transformValues(() -> new DeduplicateTransformer(Duration.ofMinutes(1)), storeSupplier.name())
                .filter((k, v) -> v != null);

        enriched.to(enrichedTopic);

        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));
        KTable<Windowed<String>, Candle> candles = enriched
                .groupBy((k, v) -> symbolFrom(v))
                .windowedBy(windows)
                .aggregate(Candle::new, (key, value, agg) -> agg.update(value), Materialized.with(Serdes.String(), candleSerde()));

        candles.toStream().mapValues(c -> c.toJson()).to(candleTopic, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    private static String normalize(String rawJson) {
        try {
            JsonNode node = MAPPER.readTree(rawJson);
            double price = node.path("price").asDouble();
            double qty = node.path("qty").asDouble();
            String symbol = node.path("symbol").asText().toUpperCase();
            long tradeId = node.path("trade_id").asLong();
            long eventTime = node.path("event_time").asLong();
            String base = symbol.substring(0, symbol.length() - 4);
            String quote = symbol.substring(symbol.length() - 4);
            double notional = price * qty;
            String session = (node.path("is_buyer_maker").asBoolean()) ? "sell" : "buy";
            Trade t = new Trade(symbol, base, quote, price, qty, notional, tradeId, eventTime, node.path("is_buyer_maker").asBoolean(), node.path("source").asText("binance"), session, node.path("ingest_ts").asLong());
            return MAPPER.writeValueAsString(t);
        } catch (Exception e) {
            return null;
        }
    }

    private static String keyFrom(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            return n.path("symbol").asText() + "-" + n.path("trade_id").asText();
        } catch (Exception e) {
            return "";
        }
    }

    private static String symbolFrom(String json) {
        try {
            return MAPPER.readTree(json).path("symbol").asText();
        } catch (Exception e) {
            return "";
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }

    private static Serde<Candle> candleSerde() {
        return Serdes.serdeFrom(new JsonPOJOSerde.Serializer<>(), new JsonPOJOSerde.Deserializer<>(Candle.class));
    }
}
