package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

const (
	defaultStreamURL = "wss://stream.binance.com:9443/stream"
	metricPort       = ":2112"
)

type aggTradeMessage struct {
	Stream string `json:"stream"`
	Data   struct {
		EventTime int64  `json:"E"`
		Symbol    string `json:"s"`
		Price     string `json:"p"`
		Qty       string `json:"q"`
		TradeID   int64  `json:"a"`
		IsMaker   bool   `json:"m"`
	} `json:"data"`
}

type rawTrade struct {
	EventTime    int64   `json:"event_time"`
	Symbol       string  `json:"symbol"`
	Price        float64 `json:"price"`
	Qty          float64 `json:"qty"`
	TradeID      int64   `json:"trade_id"`
	IsBuyerMaker bool    `json:"is_buyer_maker"`
	Source       string  `json:"source"`
	IngestedAt   int64   `json:"ingest_ts"`
}

var (
	eventsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "events_received_total",
		Help: "Total Binance websocket messages received",
	})
	eventsPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "events_published_total",
		Help: "Total Kafka messages published",
	})
	publishErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "publish_errors_total",
		Help: "Kafka publish errors",
	})
)

func main() {
	prometheus.MustRegister(eventsReceived, eventsPublished, publishErrors)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(metricPort, nil)
	}()

	streams := getenv("BINANCE_STREAMS", "btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade")
	kafkaBrokers := getenv("KAFKA_BROKERS", "kafka:9092")
	rawTopic := getenv("RAW_TOPIC", "raw_trades")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(kafkaBrokers, ","),
		Topic:   rawTopic,
	})
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	for {
		if err := consume(ctx, streams, writer); err != nil {
			log.Printf("consume error: %v", err)
			time.Sleep(time.Second * 5)
		}
		select {
		case <-interrupt:
			log.Println("shutting down")
			return
		default:
		}
	}
}

func consume(ctx context.Context, streams string, writer *kafka.Writer) error {
	url := defaultStreamURL + "?streams=" + streams
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetPongHandler(func(appData string) error { return nil })

	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingTicker.C:
			_ = conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(time.Second))
		case <-time.After(time.Second * 5):
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		eventsReceived.Inc()

		var evt aggTradeMessage
		if err := json.Unmarshal(message, &evt); err != nil {
			log.Printf("decode error: %v", err)
			continue
		}

		price, _ := strconv.ParseFloat(evt.Data.Price, 64)
		qty, _ := strconv.ParseFloat(evt.Data.Qty, 64)
		payload := rawTrade{
			EventTime:    evt.Data.EventTime,
			Symbol:       strings.ToUpper(evt.Data.Symbol),
			Price:        price,
			Qty:          qty,
			TradeID:      evt.Data.TradeID,
			IsBuyerMaker: evt.Data.IsMaker,
			Source:       "binance",
			IngestedAt:   time.Now().UnixMilli(),
		}

		b, _ := json.Marshal(payload)
		if err := writer.WriteMessages(ctx, kafka.Message{Value: b}); err != nil {
			publishErrors.Inc()
			log.Printf("write error: %v", err)
			continue
		}
		eventsPublished.Inc()
	}
}

func getenv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}
