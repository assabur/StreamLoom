package streamloom.eod;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Quote {
    public String symbol;
    public String date;
    public String time;
    public double open;
    public double high;
    public double low;
    public double close;
    public long volume;
    public String source;
    public String fetchedAt;
    public long eventTimeMs;

    public Quote copy() {
        Quote out = new Quote();
        out.symbol = symbol;
        out.date = date;
        out.time = time;
        out.open = open;
        out.high = high;
        out.low = low;
        out.close = close;
        out.volume = volume;
        out.source = source;
        out.fetchedAt = fetchedAt;
        out.eventTimeMs = eventTimeMs;
        return out;
    }
}
