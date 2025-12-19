package com.streamloom.markets;

public class Trade {
    public String symbol;
    public String base_asset;
    public String quote_asset;
    public double price;
    public double qty;
    public double notional;
    public long trade_id;
    public long event_time;
    public boolean is_buyer_maker;
    public String source;
    public String session;
    public long ingest_ts;

    public Trade() {}

    public Trade(String symbol, String base, String quote, double price, double qty, double notional, long tradeId, long eventTime, boolean isBuyer, String source, String session, long ingestTs) {
        this.symbol = symbol;
        this.base_asset = base;
        this.quote_asset = quote;
        this.price = price;
        this.qty = qty;
        this.notional = notional;
        this.trade_id = tradeId;
        this.event_time = eventTime;
        this.is_buyer_maker = isBuyer;
        this.source = source;
        this.session = session;
        this.ingest_ts = ingestTs;
    }
}
