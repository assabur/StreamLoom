package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type enrichedTrade struct {
	EventTime    int64   `json:"event_time"`
	Symbol       string  `json:"symbol"`
	BaseAsset    string  `json:"base_asset"`
	QuoteAsset   string  `json:"quote_asset"`
	Price        float64 `json:"price"`
	Qty          float64 `json:"qty"`
	Notional     float64 `json:"notional"`
	TradeID      int64   `json:"trade_id"`
	IsBuyerMaker bool    `json:"is_buyer_maker"`
	Source       string  `json:"source"`
	Session      string  `json:"session"`
	IngestedAt   int64   `json:"ingest_ts"`
}

type candle struct {
	Symbol      string  `json:"symbol"`
	WindowStart int64   `json:"window_start"`
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Volume      float64 `json:"volume"`
	TradeCount  int64   `json:"trade_count"`
	VWAP        float64 `json:"vwap"`
	Volatility  float64 `json:"volatility"`
}

var (
	consumerProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_processed_total",
		Help: "Number of Kafka records consumed",
	})
	insertLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "insert_latency_ms",
		Help:    "Latency of ClickHouse batch inserts",
		Buckets: prometheus.LinearBuckets(5, 10, 10),
	})
	insertErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "insert_errors_total",
		Help: "Number of ClickHouse insert errors",
	})
)

func main() {
	prometheus.MustRegister(consumerProcessed, insertLatency, insertErrors)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":2112", nil)
	}()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "kafka:9092"), ",")
	enrichedTopic := getenv("ENRICHED_TOPIC", "enriched_trades")
	candleTopic := getenv("CANDLE_TOPIC", "candles_1m")
	dsn := getenv("CLICKHOUSE_DSN", "clickhouse://default:@clickhouse:9000/default")

	addr := strings.TrimPrefix(dsn, "clickhouse://")
	conn, err := ch.Open(&ch.Options{Addr: []string{addr}})
	if err != nil {
		log.Fatalf("clickhouse connect error: %v", err)
	}
	defer conn.Close()

	tradeReader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "clickhouse-sink-trades", Topic: enrichedTopic, MinBytes: 1, MaxBytes: 10e6})
	candleReader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "clickhouse-sink-candles", Topic: candleTopic, MinBytes: 1, MaxBytes: 10e6})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumeTrades(ctx, conn, tradeReader)
	consumeCandles(ctx, conn, candleReader)
}

func consumeTrades(ctx context.Context, conn ch.Conn, reader *kafka.Reader) {
	batch := make([]enrichedTrade, 0, 500)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		err := conn.Exec(ctx, `INSERT INTO trades_enriched (symbol, base_asset, quote_asset, event_time, price, qty, notional, trade_id, is_buyer_maker, source, session, ingest_ts) VALUES`, batch)
		if err != nil {
			insertErrors.Inc()
			log.Printf("insert error trades: %v", err)
		}
		insertLatency.Observe(float64(time.Since(start).Milliseconds()))
		batch = batch[:0]
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush()
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("reader error: %v", err)
				continue
			}
			consumerProcessed.Inc()
			var t enrichedTrade
			if err := json.Unmarshal(msg.Value, &t); err != nil {
				log.Printf("decode error: %v", err)
				continue
			}
			batch = append(batch, t)
			if len(batch) >= 200 {
				flush()
			}
		}
	}
}

func consumeCandles(ctx context.Context, conn ch.Conn, reader *kafka.Reader) {
	batch := make([]candle, 0, 200)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		err := conn.Exec(ctx, `INSERT INTO candles_1m (symbol, window_start, open, high, low, close, volume, trade_count, vwap, volatility) VALUES`, batch)
		if err != nil {
			insertErrors.Inc()
			log.Printf("insert error candles: %v", err)
		}
		insertLatency.Observe(float64(time.Since(start).Milliseconds()))
		batch = batch[:0]
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush()
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("reader error: %v", err)
				continue
			}
			consumerProcessed.Inc()
			var c candle
			if err := json.Unmarshal(msg.Value, &c); err != nil {
				log.Printf("decode error: %v", err)
				continue
			}
			batch = append(batch, c)
			if len(batch) >= 100 {
				flush()
			}
		}
	}
}

func getenv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}
