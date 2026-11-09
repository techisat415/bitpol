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

func TestVelocityBoost(t *testing.T) {
	now := time.Unix(1000, 0)
	g := graph.New()
	for i := 0; i < 2; i++ {
		if err := g.AddEdge(graph.Edge{FromAddress: "source", ToAddress: "destination",
			TokenAddress: "token", RawAmount: big.NewInt(1), USDValue: 900,
			Timestamp: now.Add(-time.Duration(i) * time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	engine := NewRiskEngine(g, RiskConfig{
		Now:            func() time.Time { return now },
		VelocityWindow: 15 * time.Minute, VelocityThreshold: 1, VelocityBoost: 1,
	})
	engine.SetFlagged("source", true)
	velocity, err := engine.Velocity("source", now)
	if err != nil || velocity <= 1 {
		t.Fatalf("unexpected velocity=%f err=%v", velocity, err)
	}
	if multiplier := engine.velocityMultiplier("source", now); multiplier <= 1 {
		t.Fatalf("expected velocity multiplier > 1, got %f", multiplier)
	}
}
