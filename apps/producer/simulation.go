package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type simulationConfig struct {
	Symbols  string
	Key      string
	Interval time.Duration
	Seed     int64
	Rounds   int
	Step     time.Duration
	Start    time.Time
}

type quoteSimulator struct {
	symbols []string
	prices  map[string]float64
	random  *rand.Rand
}

func newQuoteSimulator(symbols string, seed int64) (*quoteSimulator, error) {
	s := &quoteSimulator{prices: make(map[string]float64), random: rand.New(rand.NewSource(seed))}
	for _, symbol := range splitCSV(symbols) {
		symbol = strings.ToUpper(symbol)
		if _, exists := s.prices[symbol]; exists {
			continue
		}
		s.prices[symbol] = 100 + 50*float64(len(s.symbols))
		s.symbols = append(s.symbols, symbol)
	}
	if len(s.symbols) == 0 {
		return nil, fmt.Errorf("no symbols provided")
	}
	return s, nil
}

// Every symbol follows its own random walk. These are synthetic interval
// prices and volumes, not cumulative daily snapshots from a market feed.
func (s *quoteSimulator) next(at time.Time) []marketQuote {
	at = at.UTC()
	quotes := make([]marketQuote, 0, len(s.symbols))
	roundPrice := func(price float64) float64 {
		return math.Max(0.01, math.Round(price*100)/100)
	}
	for _, symbol := range s.symbols {
		open := s.prices[symbol]
		close := roundPrice(open * (1 + (s.random.Float64()-0.5)*0.02))
		high := roundPrice(math.Max(open, close) * (1 + s.random.Float64()*0.005))
		low := roundPrice(math.Min(open, close) * (1 - s.random.Float64()*0.005))
		quotes = append(quotes, marketQuote{
			Symbol: symbol, Date: at.Format("2006-01-02"), Time: at.Format("15:04:05"),
			Open: open, High: high, Low: low, Close: close,
			Volume: int64(1 + s.random.Intn(1000)), Source: "simulation",
			FetchedAt: time.Now().UTC().Format(time.RFC3339),
		})
		s.prices[symbol] = close
	}
	return quotes
}

func runSimulation(ctx context.Context, writer messageWriter, config simulationConfig) error {
	if config.Interval <= 0 {
		return fmt.Errorf("poll-interval must be > 0")
	}
	if config.Rounds < 0 {
		return fmt.Errorf("simulation-rounds must be >= 0")
	}
	if config.Step < 0 {
		return fmt.Errorf("simulation-step must be >= 0")
	}
	if !config.Start.IsZero() && config.Step == 0 {
		return fmt.Errorf("simulation-start requires simulation-step > 0")
	}
	simulator, err := newQuoteSimulator(config.Symbols, config.Seed)
	if err != nil {
		return err
	}
	simulatedTime := config.Start.UTC()
	if config.Start.IsZero() {
		simulatedTime = time.Now().UTC()
	}

	for round := 0; config.Rounds == 0 || round < config.Rounds; round++ {
		if ctx.Err() != nil {
			return nil
		}
		at := simulatedTime
		if config.Step == 0 {
			at = time.Now().UTC()
		}
		messages := make([]kafka.Message, 0, len(simulator.symbols))
		for _, quote := range simulator.next(at) {
			payload, err := json.Marshal(quote)
			if err != nil {
				return err
			}
			key := config.Key
			if key == "" {
				key = quote.Symbol
			}
			messages = append(messages, kafka.Message{Key: []byte(key), Value: payload, Time: at})
		}
		if err := writer.WriteMessages(ctx, messages...); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		fmt.Fprintf(os.Stderr, "simulation: published %d quotes at %s (round %d)\n", len(messages), at.Format(time.RFC3339), round+1)
		simulatedTime = simulatedTime.Add(config.Step)
		if config.Rounds != 0 && round+1 == config.Rounds {
			return nil
		}
		timer := time.NewTimer(config.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}
