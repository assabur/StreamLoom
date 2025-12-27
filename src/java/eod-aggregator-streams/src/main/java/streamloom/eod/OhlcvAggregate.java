package streamloom.eod;

public class OhlcvAggregate {
    public String symbol;
    public String windowStart;
    public String windowEnd;
    public String windowLabel;
    public double open;
    public double high;
    public double low;
    public double close;
    public long volumeSum;
    public long count;
    public String source;
    public long firstEventTimeMs;
    public long lastEventTimeMs;
    public String ingestTs;
}
