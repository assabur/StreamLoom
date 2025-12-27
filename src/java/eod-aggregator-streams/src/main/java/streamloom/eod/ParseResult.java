package streamloom.eod;

public class ParseResult {
    public Quote quote;
    public String error;
    public String raw;

    public static ParseResult ok(Quote quote, String raw) {
        ParseResult result = new ParseResult();
        result.quote = quote;
        result.raw = raw;
        return result;
    }

    public static ParseResult error(String raw, String error) {
        ParseResult result = new ParseResult();
        result.raw = raw;
        result.error = error;
        return result;
    }
}
