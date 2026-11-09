package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/techisat415/bitpol/internal/benchmark"
)

func main() {
	baseURL := flag.String("base-url", "http://localhost:8080", "engine API URL")
	walletFile := flag.String("wallet-file", "", "newline-delimited wallet addresses exported from BigQuery")
	requests := flag.Int("requests", 10000, "number of API requests")
	concurrency := flag.Int("concurrency", 128, "maximum concurrent requests")
	output := flag.String("output", "BENCHMARKS.md", "Markdown report path")
	flag.Parse()
	wallets, err := benchmark.LoadWallets(*walletFile)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	result, err := benchmark.RunHTTP(ctx, benchmark.HTTPConfig{
		BaseURL: *baseURL, Wallets: wallets, Requests: *requests,
		Concurrency: *concurrency,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*output, []byte(benchmark.FormatReport(result, time.Now())), 0o644); err != nil {
		log.Fatal(err)
	}
}
