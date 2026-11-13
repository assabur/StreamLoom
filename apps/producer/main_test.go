package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

const quoteCSV = "Symbol,Date,Time,Open,High,Low,Close,Volume\nAAPL.US,2026-11-13,15:30:00,100,110,90,105,30\n"

type recordingWriter struct {
	messages []kafka.Message
	cancel   context.CancelFunc
}

func (w *recordingWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	if w.cancel != nil {
		w.cancel()
	}
	return nil
}

func TestFetchStooqParsesQuote(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/csv" {
			t.Error("missing CSV Accept header")
		}
		w.Write([]byte(quoteCSV))
	}))
	defer server.Close()
	quote, err := fetchStooq(context.Background(), server.Client(), server.URL, "AAPL.US")
	if err != nil {
		t.Fatal(err)
	}
	if quote.Symbol != "AAPL.US" || quote.Open != 100 || quote.High != 110 || quote.Low != 90 || quote.Close != 105 || quote.Volume != 30 || quote.Source != "stooq" {
		t.Fatalf("unexpected quote: %+v", quote)
	}
	if _, err := time.Parse(time.RFC3339, quote.FetchedAt); err != nil {
		t.Fatal(err)
	}
}

func TestFetchStooqPreservesHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	_, err := fetchStooq(context.Background(), server.Client(), server.URL, "AAPL.US")
	var statusErr *stooqHTTPError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != 404 {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFetchStooqRejectsNonCSVResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("<html>Enable JavaScript</html>\nchallenge\n"))
	}))
	defer server.Close()
	_, err := fetchStooq(context.Background(), server.Client(), server.URL, "AAPL.US")
	if err == nil || !strings.Contains(err.Error(), "expected quote CSV") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunStooqStopsWhenAllSymbolsReturn404(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	writer := &recordingWriter{}
	err := runStooq(ctx, writer, "", "AAPL.US,MSFT.US", time.Hour, server.URL+"?s=%s")
	if err == nil || !strings.Contains(err.Error(), "HTTP 404 for every symbol") {
		t.Fatalf("unexpected error: %v", err)
	}
	if requests != 2 || len(writer.messages) != 0 {
		t.Fatalf("requests=%d, messages=%d", requests, len(writer.messages))
	}
}

func TestRunStooqContinuesAfterOneMissingSymbol(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("s")
		if symbol != strings.ToLower(symbol) {
			t.Errorf("symbol must be lowercase: %s", symbol)
		}
		if symbol == "missing.us" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(quoteCSV))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	writer := &recordingWriter{cancel: cancel}
	if err := runStooq(ctx, writer, "key", "MISSING.US,AAPL.US", time.Hour, server.URL+"?s=%s"); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("messages=%d", len(writer.messages))
	}
	var quote marketQuote
	if err := json.Unmarshal(writer.messages[0].Value, &quote); err != nil {
		t.Fatal(err)
	}
	if quote.Symbol != "AAPL.US" || string(writer.messages[0].Key) != "key" {
		t.Fatalf("unexpected message: %+v", writer.messages[0])
	}
}
