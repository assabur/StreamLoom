package streamloom.eod;

import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;

public class OhlcvState {
    public double open;
    public double high;
    public double low;
    public double close;
    public long volumeSum;
    public long count;
    public long firstEventTimeMs;
    public long lastEventTimeMs;
    public String source;

    public OhlcvState update(Quote quote) {
        if (count == 0) {
            open = quote.open;
            high = quote.high;
            low = quote.low;
            close = quote.close;
            volumeSum = quote.volume;
            count = 1;
            firstEventTimeMs = quote.eventTimeMs;
            lastEventTimeMs = quote.eventTimeMs;
            source = quote.source;
            return this;
        }
        close = quote.close;
        high = Math.max(high, quote.high);
        low = Math.min(low, quote.low);
        volumeSum += quote.volume;
        count += 1;
        lastEventTimeMs = quote.eventTimeMs;
        return this;
    }

    public OhlcvAggregate toAggregate(Windowed<String> windowedKey, String windowLabel) {
        OhlcvAggregate agg = new OhlcvAggregate();
        agg.symbol = windowedKey.key();
        agg.windowStart = Instant.ofEpochMilli(windowedKey.window().start()).toString();
        agg.windowEnd = Instant.ofEpochMilli(windowedKey.window().end()).toString();
        agg.windowLabel = windowLabel;
        agg.open = open;
        agg.high = high;
        agg.low = low;
        agg.close = close;
        agg.volumeSum = volumeSum;
        agg.count = count;
        agg.source = source;
        agg.firstEventTimeMs = firstEventTimeMs;
        agg.lastEventTimeMs = lastEventTimeMs;
        agg.ingestTs = Instant.now().toString();
        return agg;
    }
}
