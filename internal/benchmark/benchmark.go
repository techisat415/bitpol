package benchmark

import (
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/techisat415/bitpol/internal/graph"
	"github.com/techisat415/bitpol/internal/risk"
)

type SyntheticConfig struct {
	TransfersPerSecond int
	PeelLength         int
	FanBranches        int
	LoopLength         int
}

func SyntheticGraph(cfg SyntheticConfig) *graph.Graph {
	if cfg.TransfersPerSecond <= 0 {
		cfg.TransfersPerSecond = 1500
	}
	if cfg.PeelLength <= 0 {
		cfg.PeelLength = 20
	}
	if cfg.FanBranches <= 0 {
		cfg.FanBranches = 100
	}
	if cfg.LoopLength <= 0 {
		cfg.LoopLength = 3
	}
	g := graph.New()
	now := time.Now().Add(-time.Minute)
	add := func(from, to string, offset int64, value float64) {
		_ = g.AddEdge(graph.Edge{FromAddress: from, ToAddress: to, TokenAddress: "synthetic",
			RawAmount: big.NewInt(1), USDValue: value, Timestamp: now.Add(time.Duration(offset) * time.Millisecond),
			TxHash: from + ":" + to})
	}
	for i := 0; i < cfg.PeelLength; i++ {
		from := "peel-" + itoa(i)
		next := "peel-" + itoa(i+1)
		add(from, next, int64(i), 10000)
		add(from, "peel-exit-"+itoa(i), int64(i), 1000)
	}
	for i := 0; i < cfg.FanBranches; i++ {
		branch := "fan-" + itoa(i)
		add("fan-source", branch, int64(i), 1000)
		add(branch, "fan-consolidation", int64(cfg.FanBranches+i), 1000)
	}
	for i := 0; i < cfg.LoopLength; i++ {
		add("loop-"+itoa(i), "loop-"+itoa((i+1)%cfg.LoopLength), int64(i), 2000)
	}
	return g
}

func itoa(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var out [20]byte
	i := len(out)
	for value > 0 {
		i--
		out[i] = digits[value%10]
		value /= 10
	}
	return string(out[i:])
}

type BenchmarkResult struct {
	Latencies   []time.Duration
	Allocations uint64
	Throughput  float64
}

type Percentiles struct {
	P50, P95, P99 time.Duration
}

func LatencyPercentiles(latencies []time.Duration) Percentiles {
	if len(latencies) == 0 {
		return Percentiles{}
	}
	ordered := append([]time.Duration(nil), latencies...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	at := func(percentile int) time.Duration {
		index := (len(ordered) - 1) * percentile / 100
		return ordered[index]
	}
	return Percentiles{P50: at(50), P95: at(95), P99: at(99)}
}

func RunTraversalLoad(g *graph.Graph, engine *risk.RiskEngine, start string, requests int, concurrency int) BenchmarkResult {
	if requests <= 0 {
		requests = 1000
	}
	if concurrency <= 0 {
		concurrency = 8
	}
	startTime := time.Now()
	latencies := make([]time.Duration, requests)
	var next sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	for i := 0; i < requests; i++ {
		next.Add(1)
		go func(index int) {
			defer next.Done()
			sem <- struct{}{}
			begin := time.Now()
			_, _ = engine.Score(start)
			latencies[index] = time.Since(begin)
			<-sem
		}(i)
	}
	next.Wait()
	return BenchmarkResult{Latencies: latencies, Throughput: float64(requests) / time.Since(startTime).Seconds()}
}
