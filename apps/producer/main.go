package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func envOrDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func main() {
	defaultBrokers := envOrDefault("BROKERS", "localhost:9092")
	defaultTopic := envOrDefault("TOPIC", "eod")
	defaultClientID := envOrDefault("CLIENT_ID", "eod-producer")
	defaultSymbols := envOrDefault("SYMBOLS", "AAPL.US")
	defaultStooqURL := envOrDefault("STOOQ_URL", "https://stooq.com/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv")

	brokersFlag := flag.String("brokers", defaultBrokers, "Kafka brokers, comma-separated (env BROKERS)")
	topicFlag := flag.String("topic", defaultTopic, "Kafka topic (env TOPIC)")
	clientIDFlag := flag.String("client-id", defaultClientID, "Kafka client id (env CLIENT_ID)")
	keyFlag := flag.String("key", "", "Message key")
	messageFlag := flag.String("message", "", "Message payload")
	countFlag := flag.Int("count", 1, "Number of messages to send")
	intervalFlag := flag.Duration("interval", 0, "Delay between messages (e.g. 250ms, 1s)")
	stdinFlag := flag.Bool("stdin", false, "Read messages from stdin, one per line")
	simulateFlag := flag.Bool("simulate", false, "Generate synthetic quotes without any external API")
	seedFlag := flag.Int64("seed", 1, "Simulation random seed")
	roundsFlag := flag.Int("simulation-rounds", 0, "Simulation rounds per symbol; 0 runs until interrupted")
	stepFlag := flag.Duration("simulation-step", 0, "Simulated time between rounds; 0 uses the current time")
	startFlag := flag.String("simulation-start", "", "Simulation start in RFC3339 (requires simulation-step > 0)")
	stooqFlag := flag.Bool("stooq", false, "Fetch from Stooq and publish normalized quotes")
	symbolsFlag := flag.String("symbols", defaultSymbols, "Symbols, comma-separated (env SYMBOLS)")
	pollFlag := flag.Duration("poll-interval", time.Second, "Interval between quote batches")
	stooqURLFlag := flag.String("stooq-url", defaultStooqURL, "Stooq URL template with %s placeholder (env STOOQ_URL)")
	flag.Parse()

	if *topicFlag == "" {
		fmt.Fprintln(os.Stderr, "topic is required")
		os.Exit(1)
	}

	modes := 0
	for _, enabled := range []bool{*stdinFlag, *stooqFlag, *simulateFlag, *messageFlag != ""} {
		if enabled {
			modes++
		}
	}
	if modes > 1 {
		fmt.Fprintln(os.Stderr, "choose one mode: -simulate, -stooq, -stdin or -message")
		os.Exit(1)
	}
	if modes == 0 {
		fmt.Fprintln(os.Stderr, "choose a mode: -simulate, -stooq, -stdin or -message")
		os.Exit(1)
	}

	brokers := strings.Split(*brokersFlag, ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}

	var balancer kafka.Balancer = &kafka.LeastBytes{}
	if *simulateFlag {
		// Keep each simulated symbol ordered when the topic has multiple partitions.
		balancer = &kafka.Hash{}
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    *topicFlag,
		Balancer: balancer,
		Dialer:   &kafka.Dialer{ClientID: *clientIDFlag},
	})
	defer func() {
		_ = writer.Close()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *simulateFlag {
		var start time.Time
		if *startFlag != "" {
			var err error
			start, err = time.Parse(time.RFC3339, *startFlag)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid simulation-start: %v\n", err)
				os.Exit(1)
			}
		}
		err := runSimulation(ctx, writer, simulationConfig{
			Symbols: *symbolsFlag, Key: *keyFlag, Interval: *pollFlag,
			Seed: *seedFlag, Rounds: *roundsFlag, Step: *stepFlag, Start: start,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "simulation failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *stooqFlag {
		if err := runStooq(ctx, writer, *keyFlag, *symbolsFlag, *pollFlag, *stooqURLFlag); err != nil {
			fmt.Fprintf(os.Stderr, "stooq failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *stdinFlag {
		sendFromStdin(ctx, writer, *keyFlag)
		return
	}

	if *countFlag < 1 {
		fmt.Fprintln(os.Stderr, "count must be >= 1")
		os.Exit(1)
	}

	for i := 0; i < *countFlag; i++ {
		payload := *messageFlag
		if *countFlag > 1 {
			payload = fmt.Sprintf("%s (%d/%d)", *messageFlag, i+1, *countFlag)
		}
		if err := writeMessage(ctx, writer, *keyFlag, payload); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write message: %v\n", err)
			os.Exit(1)
		}
		if *intervalFlag > 0 && i < *countFlag-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(*intervalFlag):
			}
		}
	}
}

func sendFromStdin(ctx context.Context, writer *kafka.Writer, key string) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if err := writeMessage(ctx, writer, key, line); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write message: %v\n", err)
			os.Exit(1)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin read error: %v\n", err)
		os.Exit(1)
	}
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

func writeMessage(ctx context.Context, writer messageWriter, key, payload string) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(payload),
		Time:  time.Now(),
	}
	return writer.WriteMessages(ctx, msg)
}

type marketQuote struct {
	Symbol    string  `json:"symbol"`
	Date      string  `json:"date"`
	Time      string  `json:"time"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int64   `json:"volume"`
	Source    string  `json:"source"`
	FetchedAt string  `json:"fetched_at"`
}

func runStooq(ctx context.Context, writer messageWriter, key, symbols string, poll time.Duration, urlTemplate string) error {
	symbolList := splitCSV(symbols)
	if len(symbolList) == 0 {
		return fmt.Errorf("no symbols provided")
	}
	if poll <= 0 {
		return fmt.Errorf("poll-interval must be > 0")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		notFound := 0
		for _, symbol := range symbolList {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			quote, err := fetchStooq(ctx, client, fmt.Sprintf(urlTemplate, url.QueryEscape(strings.ToLower(symbol))), symbol)
			if err != nil {
				fmt.Fprintf(os.Stderr, "stooq fetch error for %s: %v\n", symbol, err)
				var statusErr *stooqHTTPError
				if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
					notFound++
				}
				continue
			}
			payload, err := json.Marshal(quote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "stooq marshal error for %s: %v\n", symbol, err)
				continue
			}
			if err := writeMessage(ctx, writer, key, string(payload)); err != nil {
				return err
			}
		}

		if notFound == len(symbolList) {
			return fmt.Errorf("Stooq returned HTTP 404 for every symbol; verify the symbols and STOOQ_URL (-stooq-url), or choose another quote source; stopping polling")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type stooqHTTPError struct {
	StatusCode int
}

func (err *stooqHTTPError) Error() string {
	return fmt.Sprintf("HTTP %d (%s)", err.StatusCode, http.StatusText(err.StatusCode))
}

func fetchStooq(ctx context.Context, client *http.Client, url, symbol string) (*marketQuote, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "StreamLoom/0.1")
	req.Header.Set("Accept", "text/csv")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &stooqHTTPError{StatusCode: resp.StatusCode}
	}

	reader := csv.NewReader(resp.Body)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	record, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("empty response")
		}
		return nil, err
	}

	data := map[string]string{}
	for i, name := range header {
		if i < len(record) {
			data[name] = record[i]
		}
	}

	for _, name := range []string{"Date", "Time", "Open", "High", "Low", "Close", "Volume"} {
		if _, ok := data[name]; !ok {
			return nil, fmt.Errorf("unexpected Stooq response: missing CSV column %q (expected quote CSV)", name)
		}
	}

	openVal, err := parseFloat(data["Open"])
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	highVal, err := parseFloat(data["High"])
	if err != nil {
		return nil, fmt.Errorf("high: %w", err)
	}
	lowVal, err := parseFloat(data["Low"])
	if err != nil {
		return nil, fmt.Errorf("low: %w", err)
	}
	closeVal, err := parseFloat(data["Close"])
	if err != nil {
		return nil, fmt.Errorf("close: %w", err)
	}
	volVal, err := parseInt(data["Volume"])
	if err != nil {
		return nil, fmt.Errorf("volume: %w", err)
	}

	return &marketQuote{
		Symbol:    symbol,
		Date:      data["Date"],
		Time:      data["Time"],
		Open:      openVal,
		High:      highVal,
		Low:       lowVal,
		Close:     closeVal,
		Volume:    volVal,
		Source:    "stooq",
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func splitCSV(input string) []string {
	raw := strings.Split(input, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func parseFloat(value string) (float64, error) {
	if value == "" || value == "N/A" {
		return 0, fmt.Errorf("missing")
	}
	return strconv.ParseFloat(value, 64)
}

func parseInt(value string) (int64, error) {
	if value == "" || value == "N/A" {
		return 0, fmt.Errorf("missing")
	}
	return strconv.ParseInt(value, 10, 64)
}
