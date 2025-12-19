package com.streamloom.markets;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class TradeStreamsAppTest {
    @Test
    void testAggregationOhlc() {
        StreamsBuilder builder = new StreamsBuilder();
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("dedup-store");
        builder.addStateStore(Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Long()));
        KStream<String, String> raw = builder.stream("raw_trades");
        KStream<String, String> enriched = raw
                .mapValues(TradeStreamsApp::normalize)
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> "BTCUSDT-" + k)
                .transformValues(() -> new DeduplicateTransformer(Duration.ofMinutes(1)), storeSupplier.name())
                .filter((k, v) -> v != null);

        enriched.to("enriched_trades");

        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));
        enriched
                .groupBy((k, v) -> "BTCUSDT")
                .windowedBy(windows)
                .aggregate(Candle::new, (key, value, agg) -> agg.update(value));

        Topology topology = builder.build();
        Properties props = new Properties();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic("raw_trades", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("enriched_trades", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput(null, sample());
            assertFalse(output.isEmpty());
            String result = output.readValue();
            assertTrue(result.contains("\"symbol\":\"BTCUSDT\""));
        }
    }

    private String sample() {
        return "{\"price\":42000.0,\"qty\":0.01,\"symbol\":\"BTCUSDT\",\"trade_id\":1,\"event_time\":1700000000000,\"is_buyer_maker\":false,\"source\":\"test\",\"ingest_ts\":1700000001000}";
    }
}
