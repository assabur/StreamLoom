CREATE TABLE IF NOT EXISTS eod_agg_1m
(
    symbol String,
    window_start String,
    window_end String,
    window_label String,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume_sum UInt64,
    count UInt64,
    source Nullable(String),
    first_event_time_ms Int64,
    last_event_time_ms Int64,
    ingest_ts String
)
ENGINE = MergeTree
PARTITION BY substring(window_start, 1, 7)
ORDER BY (symbol, window_start);

CREATE TABLE IF NOT EXISTS eod_agg_1h AS eod_agg_1m;
CREATE TABLE IF NOT EXISTS eod_agg_daily AS eod_agg_1m;

CREATE TABLE IF NOT EXISTS eod_dlt
(
    raw Nullable(String),
    error String,
    ingest_ts String
)
ENGINE = MergeTree
ORDER BY ingest_ts;
