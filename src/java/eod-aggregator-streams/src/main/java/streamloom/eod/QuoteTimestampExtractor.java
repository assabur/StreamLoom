package streamloom.eod;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class QuoteTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value instanceof Quote) {
            Quote quote = (Quote) value;
            if (quote.eventTimeMs > 0) {
                return quote.eventTimeMs;
            }
        }
        return record.timestamp();
    }
}
