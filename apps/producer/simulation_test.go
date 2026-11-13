package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestSimulatorGeneratesCoherentQuotes(t *testing.T) {
	simulator, err := newQuoteSimulator(" aapl.us,MSFT.US,AAPL.US ", 42)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 13, 15, 30, 0, 0, time.FixedZone("Paris", 3600))
	previous := map[string]float64{}
	for i := 0; i < 500; i++ {
		quotes := simulator.next(at)
		if len(quotes) != 2 {
			t.Fatalf("expected two distinct symbols, got %d", len(quotes))
		}
		for _, q := range quotes {
			if q.Source != "simulation" || q.Date != "2026-11-13" || q.Time != "14:30:00" {
				t.Fatalf("unexpected metadata: %+v", q)
			}
			if q.Open <= 0 || q.Close <= 0 || q.Low <= 0 || q.High < q.Open || q.High < q.Close || q.Low > q.Open || q.Low > q.Close || q.Volume <= 0 {
				t.Fatalf("invalid OHLCV: %+v", q)
			}
			if prior, ok := previous[q.Symbol]; ok && prior != q.Open {
				t.Fatalf("discontinuous price for %s", q.Symbol)
			}
			previous[q.Symbol] = q.Close
		}
	}
}

func TestSimulatorSeedReproducesPrices(t *testing.T) {
	a, _ := newQuoteSimulator("AAPL.US", 123)
	b, _ := newQuoteSimulator("AAPL.US", 123)
	at := time.Now()
	for i := 0; i < 10; i++ {
		first, second := a.next(at)[0], b.next(at)[0]
		if first.Open != second.Open || first.Close != second.Close || first.High != second.High || first.Low != second.Low || first.Volume != second.Volume {
			t.Fatal("same seed must produce same prices and volumes")
		}
	}
}

func TestSimulationUsesSimulatedKafkaTimestamps(t *testing.T) {
	writer := &recordingWriter{}
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	config := simulationConfig{Symbols: "AAPL.US,MSFT.US", Interval: time.Nanosecond, Seed: 1, Rounds: 3, Step: time.Hour, Start: start}
	if err := runSimulation(context.Background(), writer, config); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 6 {
		t.Fatalf("messages=%d", len(writer.messages))
	}
	for i, message := range writer.messages {
		want := start.Add(time.Duration(i/2) * time.Hour)
		if !message.Time.Equal(want) {
			t.Fatalf("Kafka timestamp=%s, expected=%s", message.Time, want)
		}
		var q marketQuote
		if err := json.Unmarshal(message.Value, &q); err != nil {
			t.Fatal(err)
		}
		if q.Time != want.Format("15:04:05") || q.Date != want.Format("2006-01-02") || q.Source != "simulation" || string(message.Key) != q.Symbol {
			t.Fatalf("unexpected quote: %+v", q)
		}
	}
}

func TestSimulationUsesCurrentTimeByDefault(t *testing.T) {
	writer := &recordingWriter{}
	before := time.Now()
	if err := runSimulation(context.Background(), writer, simulationConfig{Symbols: "AAPL.US", Interval: time.Second, Rounds: 1}); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 || writer.messages[0].Time.Before(before) || writer.messages[0].Time.After(time.Now()) {
		t.Fatal("expected current Kafka timestamp")
	}
}

func TestSimulationStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &recordingWriter{cancel: cancel}
	if err := runSimulation(ctx, writer, simulationConfig{Symbols: "AAPL.US", Interval: 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("messages=%d", len(writer.messages))
	}
}

type failingWriter struct{ err error }

func (w failingWriter) WriteMessages(context.Context, ...kafka.Message) error { return w.err }

func TestSimulationReportsKafkaFailure(t *testing.T) {
	expected := errors.New("broker unavailable")
	err := runSimulation(context.Background(), failingWriter{expected}, simulationConfig{Symbols: "AAPL.US", Interval: time.Second, Rounds: 1})
	if !errors.Is(err, expected) {
		t.Fatalf("error=%v", err)
	}
}

func TestSimulationRejectsInvalidConfiguration(t *testing.T) {
	cases := []simulationConfig{
		{Symbols: " ", Interval: time.Second},
		{Symbols: "AAPL.US", Interval: 0},
		{Symbols: "AAPL.US", Interval: time.Second, Rounds: -1},
		{Symbols: "AAPL.US", Interval: time.Second, Step: -time.Second},
		{Symbols: "AAPL.US", Interval: time.Second, Start: time.Now()},
	}
	for i, config := range cases {
		writer := &recordingWriter{}
		if err := runSimulation(context.Background(), writer, config); err == nil {
			t.Fatalf("case %d accepted", i)
		}
		if len(writer.messages) != 0 {
			t.Fatalf("case %d published messages", i)
		}
	}
}
