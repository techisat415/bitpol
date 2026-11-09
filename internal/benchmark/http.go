package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type HTTPConfig struct {
	BaseURL     string
	Wallets     []string
	Requests    int
	Concurrency int
	Timeout     time.Duration
}

type HTTPResult struct {
	Latencies []time.Duration
	Failures  int
}

func LoadWallets(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read wallet list: %w", err)
	}
	var wallets []string
	for _, line := range strings.Split(string(data), "\n") {
		if wallet := strings.TrimSpace(line); wallet != "" {
			wallets = append(wallets, wallet)
		}
	}
	if len(wallets) == 0 {
		return nil, fmt.Errorf("wallet list is empty")
	}
	return wallets, nil
}

func RunHTTP(ctx context.Context, cfg HTTPConfig) (HTTPResult, error) {
	if cfg.BaseURL == "" || len(cfg.Wallets) == 0 {
		return HTTPResult{}, fmt.Errorf("base URL and wallets are required")
	}
	if cfg.Requests <= 0 {
		cfg.Requests = 1000
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 32
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	client := &http.Client{Timeout: cfg.Timeout}
	result := HTTPResult{Latencies: make([]time.Duration, cfg.Requests)}
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < cfg.Requests; i++ {
		wallet := cfg.Wallets[rng.Intn(len(cfg.Wallets))]
		endpoint := "/v1/trace/lineage?wallet=" + wallet
		if i%2 == 0 {
			endpoint = "/v1/trace/cycles?wallet=" + wallet
		}
		wg.Add(1)
		go func(index int, path string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cfg.BaseURL, "/")+path, nil)
			if err != nil {
				mu.Lock()
				result.Failures++
				mu.Unlock()
				return
			}
			start := time.Now()
			response, err := client.Do(request)
			elapsed := time.Since(start)
			if err == nil {
				_ = response.Body.Close()
			}
			mu.Lock()
			result.Latencies[index] = elapsed
			if err != nil || response.StatusCode >= http.StatusBadRequest {
				result.Failures++
			}
			mu.Unlock()
		}(i, endpoint)
	}
	wg.Wait()
	return result, nil
}

func FormatReport(result HTTPResult, generatedAt time.Time) string {
	percentiles := LatencyPercentiles(result.Latencies)
	return fmt.Sprintf("# Mainnet API Benchmark\n\nGenerated: `%s`\n\n| Metric | Value |\n|---|---:|\n| Requests | %d |\n| Failures | %d |\n| P50 | %s |\n| P95 | %s |\n| P99 | %s |\n",
		generatedAt.UTC().Format(time.RFC3339), len(result.Latencies), result.Failures,
		percentiles.P50, percentiles.P95, percentiles.P99)
}
