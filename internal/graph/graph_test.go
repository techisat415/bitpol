package graph

import (
	"log/slog"
	"math/big"
	"testing"
	"time"
)

func edge(from, to string, at time.Time, usd float64) Edge {
	return Edge{
		FromAddress: from, ToAddress: to, TokenAddress: "0xtoken",
		RawAmount: big.NewInt(1), USDValue: usd, Timestamp: at, TxHash: from + to,
	}
}

func TestTraversePrunesTemporalInversion(t *testing.T) {
	g := New()
	t1 := time.Unix(100, 0)
	t2 := time.Unix(90, 0)
	if err := g.AddEdge(edge("a", "b", t1, 2000)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(edge("b", "c", t2, 2000)); err != nil {
		t.Fatal(err)
	}
	result, err := g.Traverse("a", Options{StartTime: time.Unix(0, 0), EndTime: time.Unix(200, 0)}, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range result.Paths {
		if len(path.Steps) > 1 {
			t.Fatalf("temporal inversion was traversed: %+v", path)
		}
	}
}

func TestTraverseExchangeIsTerminal(t *testing.T) {
	g := New()
	now := time.Unix(100, 0)
	if err := g.SetMetadata("exchange", "VERIFIED", EntityExchange); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(edge("a", "exchange", now, 2000)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(edge("exchange", "b", now, 2000)); err != nil {
		t.Fatal(err)
	}
	result, err := g.Traverse("a", Options{StartTime: time.Unix(0, 0), EndTime: time.Unix(200, 0)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.OffRampExits) != 1 || result.OffRampExits[0] != "exchange" {
		t.Fatalf("exchange was not intercepted: %+v", result.OffRampExits)
	}
	if len(result.Paths) != 1 || !result.Paths[0].TerminalSink {
		t.Fatalf("expected terminal path: %+v", result.Paths)
	}
}

func TestDualTrunkSplitting(t *testing.T) {
	g := New()
	now := time.Unix(100, 0)
	if err := g.AddEdge(edge("a", "main", now, 9000)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(edge("a", "slice", now, 1000)); err != nil {
		t.Fatal(err)
	}
	result, err := g.Traverse("a", Options{
		StartTime: time.Unix(0, 0), EndTime: time.Unix(200, 0),
		InitialInflowUSD: 10000,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SinkChecks) != 1 || result.SinkChecks[0].Edge.ToAddress != "slice" {
		t.Fatalf("secondary slice was not deferred: %+v", result.SinkChecks)
	}
	if len(result.Paths) != 1 || result.Paths[0].Score != 1 {
		t.Fatalf("primary trunk decayed unexpectedly: %+v", result.Paths)
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
