package com.streamloom.markets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Candle {
    public String symbol;
    public long window_start;
    public double open;
    public double high;
    public double low;
    public double close;
    public double volume;
    public long trade_count;
    public double vwap;
    public double volatility;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public Candle update(String json) {
        try {
            Trade t = MAPPER.readValue(json, Trade.class);
            if (trade_count == 0) {
                open = t.price;
                low = t.price;
            }
            symbol = t.symbol;
            window_start = t.event_time - (t.event_time % 60000);
            high = Math.max(high, t.price);
            low = Math.min(low, t.price);
            close = t.price;
            volume += t.qty;
            trade_count += 1;
            vwap = ((vwap * (volume - t.qty)) + (t.price * t.qty)) / volume;
            volatility = high - low;
        } catch (Exception ignored) {
        }
        return this;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }
    }
}
