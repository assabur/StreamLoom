package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests served",
	})
	httpLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_latency_ms",
		Help:    "Latency of HTTP handlers",
		Buckets: prometheus.LinearBuckets(5, 10, 10),
	})
)

func main() {
	prometheus.MustRegister(httpRequests, httpLatency)
	port := getenv("API_PORT", "8088")
	dsn := getenv("CLICKHOUSE_DSN", "clickhouse://default:@clickhouse:9000/default")
	addr := getenv("API_ADDR", "0.0.0.0")

	conn, err := ch.Open(&ch.Options{Addr: []string{stripProtocol(dsn)}})
	if err != nil {
		log.Fatalf("clickhouse connect error: %v", err)
	}
	defer conn.Close()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Get("/symbols", withMetrics(func(w http.ResponseWriter, r *http.Request) {
		resp := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
		json.NewEncoder(w).Encode(resp)
	}))
	r.Get("/candles", withMetrics(func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")
		if symbol == "" || from == "" || to == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		rows, err := conn.Query(r.Context(), `SELECT symbol, window_start, open, high, low, close, volume, trade_count, vwap, volatility FROM candles_1m WHERE symbol = ? AND window_start BETWEEN parseDateTimeBestEffort(?) AND parseDateTimeBestEffort(?) ORDER BY window_start`, symbol, from, to)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		var out []map[string]any
		for rows.Next() {
			var (
				s                                string
				ws                               time.Time
				op, hi, lo, cl, vol, vwap, volat float64
				cnt                              int64
			)
			if err := rows.Scan(&s, &ws, &op, &hi, &lo, &cl, &vol, &cnt, &vwap, &volat); err != nil {
				continue
			}
			out = append(out, map[string]any{
				"symbol":       s,
				"window_start": ws,
				"open":         op,
				"high":         hi,
				"low":          lo,
				"close":        cl,
				"volume":       vol,
				"trade_count":  cnt,
				"vwap":         vwap,
				"volatility":   volat,
			})
		}
		json.NewEncoder(w).Encode(out)
	}))
	r.Get("/stats/volume", withMetrics(func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		window := r.URL.Query().Get("window")
		if symbol == "" || window == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		rows, err := conn.Query(r.Context(), `SELECT sum(volume) FROM candles_1m WHERE symbol = ? AND window_start >= now() - toIntervalSecond(?)`, symbol, parseInterval(window))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		if rows.Next() {
			var val float64
			rows.Scan(&val)
			json.NewEncoder(w).Encode(map[string]any{"symbol": symbol, "window": window, "volume": val})
		}
	}))
	r.Get("/stats/spikes", withMetrics(func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		thresholdStr := r.URL.Query().Get("threshold")
		threshold, _ := strconv.ParseFloat(thresholdStr, 64)
		if symbol == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		rows, err := conn.Query(r.Context(), `SELECT window_start, high - low AS range FROM candles_1m WHERE symbol = ? AND range >= ? ORDER BY window_start DESC LIMIT 20`, symbol, threshold)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		var out []map[string]any
		for rows.Next() {
			var ws time.Time
			var rng float64
			if err := rows.Scan(&ws, &rng); err != nil {
				continue
			}
			out = append(out, map[string]any{"window_start": ws, "range": rng})
		}
		json.NewEncoder(w).Encode(out)
	}))

	log.Printf("API listening on %s:%s", addr, port)
	if err := http.ListenAndServe(addr+":"+port, r); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func withMetrics(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpRequests.Inc()
		start := time.Now()
		fn(w, r)
		httpLatency.Observe(float64(time.Since(start).Milliseconds()))
	}
}

func stripProtocol(dsn string) string {
	return strings.TrimPrefix(strings.TrimPrefix(dsn, "clickhouse://"), "http://")
}

func getenv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func parseInterval(window string) int {
	d, _ := time.ParseDuration(window)
	return int(d.Seconds())
}
