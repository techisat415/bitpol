// Package risk implements real-time risk propagation.
// coordination for the network contamination layer.
package risk

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/techisat415/bitpol/internal/graph"
)

var ErrWalletMissing = errors.New("wallet is not present in graph")

type RiskConfig struct {
	Lambda    float64
	MaxHops   int
	Now       func() time.Time
	PathScore func(*graph.Edge) float64
}

func (c RiskConfig) defaults() RiskConfig {
	if c.Lambda <= 0 {
		c.Lambda = 0.000001
	}
	if c.MaxHops <= 0 {
		c.MaxHops = 3
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.PathScore == nil {
		c.PathScore = func(*graph.Edge) float64 { return 1 }
	}
	return c
}

type RiskEngine struct {
	graph   *graph.Graph
	config  RiskConfig
	sdn     map[string]struct{}
	flagged map[string]struct{}
	mu      sync.RWMutex
}

func NewRiskEngine(g *graph.Graph, config RiskConfig) *RiskEngine {
	config = config.defaults()
	return &RiskEngine{
		graph: g, config: config,
		sdn:     make(map[string]struct{}),
		flagged: make(map[string]struct{}),
	}
}

func (e *RiskEngine) Graph() *graph.Graph { return e.graph }

func (e *RiskEngine) IntrinsicRisk(address string) float64 {
	return e.intrinsic(address)
}

func (e *RiskEngine) SetSanctioned(address string, value bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if value {
		e.sdn[address] = struct{}{}
	} else {
		delete(e.sdn, address)
	}
}

func (e *RiskEngine) SetFlagged(address string, value bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if value {
		e.flagged[address] = struct{}{}
	} else {
		delete(e.flagged, address)
	}
}

func (e *RiskEngine) intrinsic(address string) float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if _, ok := e.sdn[address]; ok {
		return 1
	}
	if _, ok := e.flagged[address]; ok {
		return 0.8
	}
	return 0
}

// Score computes R(w), recursively evaluating incoming edges up to MaxHops.
// The active set prevents cyclic paths from recursively amplifying a score.
func (e *RiskEngine) Score(address string) (float64, error) {
	if _, ok := e.graph.Node(address); !ok {
		return 0, ErrWalletMissing
	}
	return e.score(address, e.config.MaxHops, make(map[string]bool)), nil
}

func (e *RiskEngine) score(address string, hops int, active map[string]bool) float64 {
	risk := e.intrinsic(address)
	if risk >= 1 || hops == 0 || active[address] {
		return risk
	}
	active[address] = true
	defer delete(active, address)

	now := e.config.Now()
	incoming, _ := e.graph.Incoming(address, time.Time{}, now)
	for _, edge := range incoming {
		age := now.Sub(edge.Timestamp).Seconds()
		if age < 0 {
			age = 0
		}
		decay := math.Exp(-e.config.Lambda * age)
		centrality := 1 / math.Log2(float64(edgeSourceDegree(e.graph, edge.FromAddress))+1)
		pathScore := e.config.PathScore(edge)
		if pathScore < 0 {
			pathScore = 0
		}
		risk += e.score(edge.FromAddress, hops-1, active) * pathScore * centrality * decay
		if risk >= 1 {
			return 1
		}
	}
	return math.Min(1, risk)
}

func edgeSourceDegree(g *graph.Graph, address string) uint64 {
	node, ok := g.Node(address)
	if !ok {
		return 0
	}
	return node.OutDegree()
}

func DegreeCentrality(outDegree uint64) float64 {
	if outDegree == 0 {
		return 1
	}
	return 1 / math.Log2(float64(outDegree)+1)
}

func ExponentialDecay(lambda float64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 1
	}
	return math.Exp(-lambda * elapsed.Seconds())
}
