package streamloom.eod;

public class DltRecord {
    public String raw;
    public String error;
    public String ingestTs;

    public DltRecord(String raw, String error, String ingestTs) {
        this.raw = raw;
        this.error = error;
        this.ingestTs = ingestTs;
    }
}
