package benchmark

import (
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/techisat415/bitpol/internal/graph"
	"github.com/techisat415/bitpol/internal/risk"
)

func TestSyntheticPatternsAndP99(t *testing.T) {
	g := SyntheticGraph(SyntheticConfig{PeelLength: 20, FanBranches: 100, LoopLength: 3})
	if _, ok := g.Node("peel-20"); !ok {
		t.Fatal("peel pattern missing")
	}

	engine := risk.NewRiskEngine(g, risk.RiskConfig{MaxHops: 3})
	engine.SetFlagged("peel-0", true)
	result := RunTraversalLoad(g, engine, "peel-20", 100, 4)
	sort.Slice(result.Latencies, func(i, j int) bool { return result.Latencies[i] < result.Latencies[j] })
	p99 := result.Latencies[len(result.Latencies)*99/100]
	if p99 > 50*time.Millisecond {
		t.Fatalf("p99=%s exceeds target", p99)
	}

}

func BenchmarkRiskScore(b *testing.B) {
	g := SyntheticGraph(SyntheticConfig{PeelLength: 20, FanBranches: 100})
	engine := risk.NewRiskEngine(g, risk.RiskConfig{})
	engine.SetFlagged("peel-0", true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Score("peel-20")
	}
}

func TestSyntheticCapacityEdges(t *testing.T) {
	g := graph.New()
	for i := 0; i < 1500; i++ {
		_ = g.AddEdge(graph.Edge{FromAddress: "source", ToAddress: "to", TokenAddress: "t",
			RawAmount: big.NewInt(1), USDValue: 1, Timestamp: time.Now(), TxHash: itoa(i)})
	}
	if _, ok := g.Node("source"); !ok {
		t.Fatal("source missing")
	}
}
