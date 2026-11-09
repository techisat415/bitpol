package phase3

import (
	"math/big"
	"testing"
	"time"

	"github.com/techisat415/bitpol/graph"
)

func e(from, to string, t int64, value float64) graph.Edge {
	return graph.Edge{FromAddress: from, ToAddress: to, TokenAddress: "token",
		RawAmount: big.NewInt(1), USDValue: value, Timestamp: time.Unix(t, 0), TxHash: from + to}
}

func TestPeeling(t *testing.T) {
	g := graph.New()
	for i := int64(0); i < 6; i++ {
		from := "n" + string(rune('0'+i))
		to := "n" + string(rune('0'+i+1))
		if err := g.AddEdge(e(from, to, i+1, 100)); err != nil {
			t.Fatal(err)
		}
		if err := g.AddEdge(e(from, "peel"+string(rune('0'+i)), i+1, 10)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := DetectPeeling(g, "n1", PeelingConfig{Start: time.Unix(0, 0), End: time.Unix(10, 0), MinLength: 5})
	if err != nil || len(got) != 1 {
		t.Fatalf("peeling got=%+v err=%v", got, err)
	}
}

func TestRingAndBackwardTrace(t *testing.T) {
	g := graph.New()
	for _, edge := range []graph.Edge{e("a", "b", 1, 100), e("b", "c", 2, 100), e("c", "a", 3, 100)} {
		if err := g.AddEdge(edge); err != nil {
			t.Fatal(err)
		}
	}
	rings, err := DetectRings(g, "a", RingConfig{Start: time.Unix(0, 0), End: time.Unix(10, 0)})
	if err != nil || len(rings) != 1 {
		t.Fatalf("rings=%+v err=%v", rings, err)
	}
	traces, err := TraceBackward(g, "c", TraceConfig{Start: time.Unix(0, 0), End: time.Unix(10, 0)})
	if err != nil || len(traces) == 0 {
		t.Fatalf("traces=%+v err=%v", traces, err)
	}
}

func TestMixerConfidence(t *testing.T) {
	now := time.Unix(100, 0)
	got := CorrelateMixer(MixerConfig{MixerAddresses: map[string]string{"m": "SINK", "m2": "SOURCE"}},
		[]MixerEvent{{Address: "m", Wallet: "deposit-wallet", Cluster: "cluster-1", Token: "token", Amount: "100", Timestamp: now, GasPriceWei: "7"}},
		[]MixerEvent{{Address: "m2", Wallet: "withdrawal-wallet", Cluster: "cluster-1", Token: "token", Amount: "100", Timestamp: now.Add(time.Hour), GasPriceWei: "7"}})
	if len(got) != 1 || got[0].Confidence <= .05 {
		t.Fatalf("mixer matches=%+v", got)
	}
}
