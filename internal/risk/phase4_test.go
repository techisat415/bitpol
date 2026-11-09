package risk

import (
	"math/big"
	"testing"
	"time"

	"github.com/techisat415/bitpol/internal/graph"
)

func TestRiskPropagationAndDecay(t *testing.T) {
	g := graph.New()
	now := time.Unix(1000, 0)
	if err := g.AddEdge(graph.Edge{FromAddress: "bad", ToAddress: "clean",
		TokenAddress: "token", RawAmount: big.NewInt(1), USDValue: 1000, Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	engine := NewRiskEngine(g, RiskConfig{Now: func() time.Time { return now }, Lambda: 1})
	engine.SetFlagged("bad", true)
	score, err := engine.Score("clean")
	if err != nil {
		t.Fatal(err)
	}
	if score <= 0.79 || score > 0.8 {
		t.Fatalf("unexpected risk score %f", score)
	}
}

func TestDegreeCentralityBounds(t *testing.T) {
	if got := DegreeCentrality(0); got != 1 {
		t.Fatal(got)
	}
	if got := DegreeCentrality(1); got != 1 {
		t.Fatalf("got %f", got)
	}
	if got := DegreeCentrality(5000); got <= 0 || got >= 0.2 {
		t.Fatalf("got %f", got)
	}
}
