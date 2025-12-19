CREATE TABLE IF NOT EXISTS trades_enriched
(
    symbol String,
    base_asset String,
    quote_asset String,
    event_time DateTime64(3),
    price Float64,
    qty Float64,
    notional Float64,
    trade_id UInt64,
    is_buyer_maker UInt8,
    source String,
    session String,
    ingest_ts DateTime64(3),
    _ingest_partition UInt16 MATERIALIZED cityHash64(symbol) % 16
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (symbol, event_time, trade_id)
TTL event_time + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS candles_1m
(
    symbol String,
    window_start DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count UInt64,
    vwap Float64,
    volatility Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, window_start);

-- Sample queries
-- 1. Latest candles per symbol
SELECT * FROM candles_1m WHERE symbol IN ('BTCUSDT','ETHUSDT','SOLUSDT') ORDER BY window_start DESC LIMIT 5;
-- 2. Top volume symbols last hour
SELECT symbol, sum(volume) AS vol FROM candles_1m WHERE window_start >= now() - INTERVAL 1 HOUR GROUP BY symbol ORDER BY vol DESC;
-- 3. VWAP by symbol last 15 minutes
SELECT symbol, avg(vwap) AS vwap_15m FROM candles_1m WHERE window_start >= now() - INTERVAL 15 MINUTE GROUP BY symbol;
-- 4. Notional traded last day
SELECT symbol, sum(notional) FROM trades_enriched WHERE event_time >= now() - INTERVAL 1 DAY GROUP BY symbol;
-- 5. Simple volatility range
SELECT symbol, max(high - low) AS range_1h FROM candles_1m WHERE window_start >= now() - INTERVAL 1 HOUR GROUP BY symbol;
