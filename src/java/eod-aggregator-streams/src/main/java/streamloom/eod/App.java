package streamloom.eod;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Properties;

public final class App {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public static void main(String[] args) {
        String brokers = envOrDefault("BROKERS", "localhost:9092");
        String appId = envOrDefault("APP_ID", "eod-aggregator-streams");
        String inputTopic = envOrDefault("INPUT_TOPIC", "eod");
        String output1mTopic = envOrDefault("OUTPUT_1M_TOPIC", "eod-agg-1m");
        String outputDailyTopic = envOrDefault("OUTPUT_DAILY_TOPIC", "eod-agg-daily");
        String dltTopic = envOrDefault("DLT_TOPIC", "eod-dlt");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, QuoteTimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        JsonSerde<Quote> quoteSerde = new JsonSerde<>(Quote.class, MAPPER);
        JsonSerde<OhlcvState> stateSerde = new JsonSerde<>(OhlcvState.class, MAPPER);

        KStream<String, String> raw = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));

        KStream<String, ParseResult> parsed = raw.mapValues(App::parseQuote);

        @SuppressWarnings("unchecked")
        KStream<String, ParseResult>[] branches = parsed.branch(
                (key, value) -> value.error == null,
                (key, value) -> true
        );

        KStream<String, Quote> valid = branches[0].mapValues(result -> result.quote);
        KStream<String, DltRecord> invalid = branches[1].mapValues(result ->
                new DltRecord(result.raw, result.error, Instant.now().toString())
        );

        invalid.mapValues(App::toJson).to(dltTopic, Produced.with(stringSerde, stringSerde));

        KStream<String, Quote> normalized = valid
                .mapValues(App::normalizeQuote)
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> value.symbol);

        TimeWindows oneMinuteWindows = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(1));
        TimeWindows dailyWindows = TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofMinutes(5));

        KStream<String, String> agg1m = buildOhlcvPipeline(normalized, oneMinuteWindows, quoteSerde, stateSerde, "1m");
        agg1m.to(output1mTopic, Produced.with(stringSerde, stringSerde));

        KStream<String, String> aggDaily = buildOhlcvPipeline(normalized, dailyWindows, quoteSerde, stateSerde, "1d");
        aggDaily.to(outputDailyTopic, Produced.with(stringSerde, stringSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    private static KStream<String, String> buildOhlcvPipeline(
            KStream<String, Quote> input,
            TimeWindows windows,
            JsonSerde<Quote> quoteSerde,
            JsonSerde<OhlcvState> stateSerde,
            String windowLabel
    ) {
        KTable<Windowed<String>, OhlcvState> table = input
                .groupByKey(Grouped.with(Serdes.String(), quoteSerde))
                .windowedBy(windows)
                .aggregate(
                        OhlcvState::new,
                        (key, quote, state) -> state.update(quote),
                        Materialized.with(Serdes.String(), stateSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        return table.toStream().map((windowedKey, state) -> {
            OhlcvAggregate agg = state.toAggregate(windowedKey, windowLabel);
            return KeyValue.pair(windowedKey.key(), toJson(agg));
        });
    }

    private static ParseResult parseQuote(String raw) {
        if (raw == null || raw.isBlank()) {
            return ParseResult.error(raw, "empty_payload");
        }
        try {
            Quote quote = MAPPER.readValue(raw, Quote.class);
            String validationError = validateQuote(quote);
            if (validationError != null) {
                return ParseResult.error(raw, validationError);
            }
            return ParseResult.ok(quote, raw);
        } catch (Exception e) {
            return ParseResult.error(raw, "parse_error: " + e.getMessage());
        }
    }

    private static String validateQuote(Quote quote) {
        if (quote == null) {
            return "null_quote";
        }
        if (isBlank(quote.symbol)) {
            return "missing_symbol";
        }
        if (isBlank(quote.date) || isBlank(quote.time)) {
            return "missing_datetime";
        }
        if (Double.isNaN(quote.open) || Double.isNaN(quote.high)
                || Double.isNaN(quote.low) || Double.isNaN(quote.close)) {
            return "invalid_ohlc";
        }
        if (quote.volume < 0) {
            return "invalid_volume";
        }
        return null;
    }

    private static Quote normalizeQuote(Quote quote) {
        if (quote == null) {
            return null;
        }
        Quote normalized = quote.copy();
        normalized.symbol = quote.symbol.toUpperCase(Locale.ROOT);
        normalized.eventTimeMs = parseEventTimeMs(quote);
        if (isBlank(normalized.source)) {
            normalized.source = "stooq";
        }
        if (normalized.eventTimeMs == 0L) {
            normalized.eventTimeMs = System.currentTimeMillis();
        }
        return normalized;
    }

    private static long parseEventTimeMs(Quote quote) {
        try {
            LocalDate date = LocalDate.parse(quote.date);
            LocalTime time = LocalTime.parse(quote.time);
            LocalDateTime dateTime = LocalDateTime.of(date, time);
            return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            if (!isBlank(quote.fetchedAt)) {
                try {
                    return Instant.parse(quote.fetchedAt).toEpochMilli();
                } catch (DateTimeParseException ignoredAgain) {
                    return 0L;
                }
            }
            return 0L;
        }
    }

    private static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            return "{\"error\":\"serialization_failed\"}";
        }
    }

    private static String envOrDefault(String key, String def) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? def : value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
