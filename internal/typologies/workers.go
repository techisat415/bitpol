// Package typologies provides independent, asynchronous-friendly financial crime
// detectors over the Phase 2 graph. Each detector is stateless per call.
package typologies

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/techisat415/bitpol/internal/graph"
)

var ErrTargetMissing = errors.New("target wallet is not present in graph")

type PeelingConfig struct {
	MinLength  int
	Start, End time.Time
}

type PeelLineage struct {
	ChangePath     []string  `json:"change_path"`
	PeeledOffRamps []string  `json:"peeled_off_ramps"`
	Edges          []EdgeRef `json:"edges"`
}

func DetectPeeling(g *graph.Graph, start string, cfg PeelingConfig) ([]PeelLineage, error) {
	if _, ok := g.Node(start); !ok {
		return nil, ErrTargetMissing
	}
	if cfg.MinLength < 1 {
		cfg.MinLength = 5
	}
	var results []PeelLineage
	current := start
	lineage := PeelLineage{ChangePath: []string{start}}
	for {
		node, ok := g.Node(current)
		if !ok || node.InDegree() != 1 || node.OutDegree() != 2 {
			break
		}
		incoming, _ := g.Incoming(current, cfg.Start, cfg.End)
		outgoing, _ := g.Outgoing(current, cfg.Start, cfg.End)
		if len(incoming) != 1 || len(outgoing) != 2 || incoming[0].USDValue <= 0 {
			break
		}
		change, peel, ok := peelOutputs(outgoing, incoming[0].USDValue)
		if !ok {
			break
		}
		allEdges := append([]*graph.Edge{incoming[0]}, outgoing...)
		lineage.Edges = append(lineage.Edges, edgeRefs(allEdges...)...)
		lineage.PeeledOffRamps = append(lineage.PeeledOffRamps, peel.ToAddress)
		lineage.ChangePath = append(lineage.ChangePath, change.ToAddress)
		current = change.ToAddress
		if len(lineage.ChangePath)-1 >= cfg.MinLength {
			results = append(results, lineage)
			lineage = PeelLineage{ChangePath: append([]string(nil), lineage.ChangePath...)}
		}
	}
	return results, nil
}

func peelOutputs(edges []*graph.Edge, input float64) (change, peel *graph.Edge, ok bool) {
	for _, candidate := range edges {
		for _, other := range edges {
			if candidate == other {
				continue
			}
			if candidate.USDValue >= input*0.8 && other.USDValue < input*0.2 {
				return candidate, other, true
			}
		}
	}
	return nil, nil, false
}

type FanConfig struct {
	Start, End  time.Time
	MinBranches int
	MaxBranches int
}

type FanPattern struct {
	Source         string    `json:"source"`
	Branches       []string  `json:"branches"`
	Consolidations []string  `json:"consolidations"`
	Edges          []EdgeRef `json:"edges"`
}

func DetectFanOutIn(g *graph.Graph, source string, cfg FanConfig) ([]FanPattern, error) {
	if _, ok := g.Node(source); !ok {
		return nil, ErrTargetMissing
	}
	if cfg.MinBranches < 2 {
		cfg.MinBranches = 2
	}
	outs, _ := g.Outgoing(source, cfg.Start, cfg.End)
	if len(outs) < cfg.MinBranches {
		return nil, nil
	}
	if cfg.MaxBranches > 0 && len(outs) > cfg.MaxBranches {
		outs = outs[:cfg.MaxBranches]
	}
	branches := make(map[string][]*graph.Edge)
	for _, out := range outs {
		branches[out.ToAddress] = append(branches[out.ToAddress], out)
	}
	counts := make(map[string]int)
	for _, branch := range outs {
		incoming, _ := g.Incoming(branch.ToAddress, cfg.Start, cfg.End)
		for _, in := range incoming {
			if !in.Timestamp.Before(branch.Timestamp) {
				continue
			}
			// A consolidation candidate must receive from the branch.
			next, _ := g.Outgoing(branch.ToAddress, branch.Timestamp, cfg.End)
			for _, edge := range next {
				counts[edge.ToAddress]++
			}
		}
	}
	var patterns []FanPattern
	for consolidation, count := range counts {
		if count < cfg.MinBranches {
			continue
		}
		var refs []EdgeRef
		var names []string
		for branch := range branches {
			next, _ := g.Outgoing(branch, cfg.Start, cfg.End)
			for _, edge := range next {
				if edge.ToAddress == consolidation {
					names = append(names, branch)
					refs = append(refs, edgeRefs(edge)[0])
				}
			}
		}
		sort.Strings(names)
		patterns = append(patterns, FanPattern{Source: source, Branches: names,
			Consolidations: []string{consolidation}, Edges: refs})
	}
	return patterns, nil
}

type RingConfig struct {
	Start, End time.Time
	MaxDepth   int
}

type Ring struct {
	Addresses []string  `json:"addresses"`
	Edges     []EdgeRef `json:"edges"`
}

func DetectRings(g *graph.Graph, start string, cfg RingConfig) ([]Ring, error) {
	if _, ok := g.Node(start); !ok {
		return nil, ErrTargetMissing
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 8
	}
	var rings []Ring
	seen := map[string]bool{start: true}
	var walk func(string, time.Time, []string, []EdgeRef)
	walk = func(node string, previous time.Time, addresses []string, refs []EdgeRef) {
		if len(addresses) > cfg.MaxDepth+1 {
			return
		}
		outs, _ := g.Outgoing(node, cfg.Start, cfg.End)
		for _, edge := range outs {
			if edge.Timestamp.Before(previous) {
				continue
			}
			ref := edgeRefs(edge)[0]
			if edge.ToAddress == start && len(addresses) >= 3 {
				rings = append(rings, Ring{Addresses: append(append([]string(nil), addresses...), start),
					Edges: append(append([]EdgeRef(nil), refs...), ref)})
				continue
			}
			if seen[edge.ToAddress] {
				continue
			}
			seen[edge.ToAddress] = true
			walk(edge.ToAddress, edge.Timestamp, append(addresses, edge.ToAddress), append(refs, ref))
			delete(seen, edge.ToAddress)
		}
	}
	walk(start, cfg.Start, []string{start}, nil)
	return rings, nil
}

type MixerEvent struct {
	Address     string
	Wallet      string
	Cluster     string
	Token       string
	Amount      string
	Timestamp   time.Time
	GasPriceWei string
	Relayer     string
}

type MixerConfig struct {
	MixerAddresses map[string]string // address -> SINK or SOURCE
	MaxDelay       time.Duration
	MinConfidence  float64
}

type VirtualEdge struct {
	Deposit, Withdrawal MixerEvent
	Confidence          float64
	Reasons             []string
}

func CorrelateMixer(cfg MixerConfig, deposits, withdrawals []MixerEvent) []VirtualEdge {
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * 24 * time.Hour
	}
	if cfg.MinConfidence <= 0 {
		cfg.MinConfidence = 0.05
	}
	var matches []VirtualEdge
	for _, deposit := range deposits {
		if cfg.MixerAddresses[deposit.Address] != "SINK" {
			continue
		}
		for _, withdrawal := range withdrawals {
			if cfg.MixerAddresses[withdrawal.Address] != "SOURCE" || withdrawal.Timestamp.Before(deposit.Timestamp) {
				continue
			}
			delta := withdrawal.Timestamp.Sub(deposit.Timestamp)
			if delta > cfg.MaxDelay || deposit.Token != withdrawal.Token || deposit.Amount != withdrawal.Amount {
				continue
			}
			score := 0.4 * (1 - float64(delta)/float64(cfg.MaxDelay))
			reasons := []string{"denomination", "token", "temporal_window"}
			if deposit.GasPriceWei != "" && deposit.GasPriceWei == withdrawal.GasPriceWei {
				score += 0.25
				reasons = append(reasons, "gas_price")
			}
			if deposit.Relayer != "" && deposit.Relayer == withdrawal.Relayer {
				score += 0.2
				reasons = append(reasons, "relayer")
			}
			if deposit.Cluster != "" && deposit.Cluster == withdrawal.Cluster {
				score += 0.15
				reasons = append(reasons, "withdrawal_cluster")
			}
			score = math.Min(score, 0.99)
			if score > cfg.MinConfidence {
				matches = append(matches, VirtualEdge{Deposit: deposit, Withdrawal: withdrawal,
					Confidence: score, Reasons: reasons})
			}
		}
	}
	return matches
}

type TraceConfig struct {
	Start, End time.Time
	MaxDepth   int
}
type Trace struct {
	Source   string    `json:"source"`
	Sink     string    `json:"sink"`
	Edges    []EdgeRef `json:"edges"`
	TotalUSD float64   `json:"total_usd"`
}

func TraceBackward(g *graph.Graph, sink string, cfg TraceConfig) ([]Trace, error) {
	if _, ok := g.Node(sink); !ok {
		return nil, ErrTargetMissing
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 8
	}
	type state struct {
		node    string
		outTime time.Time
		depth   int
		refs    []EdgeRef
		total   float64
	}
	queue := []state{{node: sink, outTime: cfg.End}}
	var result []Trace
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.depth >= cfg.MaxDepth || current.node == sink && current.depth > 0 {
			if len(current.refs) > 0 {
				result = append(result, Trace{Source: current.node, Sink: sink, Edges: reverseRefs(current.refs), TotalUSD: current.total})
			}
			continue
		}
		ins, _ := g.Incoming(current.node, cfg.Start, current.outTime)
		if len(ins) == 0 && len(current.refs) > 0 {
			result = append(result, Trace{Source: current.node, Sink: sink, Edges: reverseRefs(current.refs), TotalUSD: current.total})
		}
		for _, edge := range ins {
			refs := append(append([]EdgeRef(nil), current.refs...), edgeRefs(edge)[0])
			queue = append(queue, state{node: edge.FromAddress, outTime: edge.Timestamp,
				depth: current.depth + 1, refs: refs, total: current.total + edge.USDValue})
		}
	}
	return result, nil
}

func edgeRefs(edges ...*graph.Edge) []EdgeRef {
	result := make([]EdgeRef, 0, len(edges))
	for _, edge := range edges {
		result = append(result, EdgeRef{From: edge.FromAddress, To: edge.ToAddress,
			TxHash: edge.TxHash, Token: edge.TokenAddress, USDValue: edge.USDValue, Timestamp: edge.Timestamp})
	}
	return result
}

func reverseRefs(refs []EdgeRef) []EdgeRef {
	result := append([]EdgeRef(nil), refs...)
	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp.Before(result[j].Timestamp) })
	return result
}

func NewAlert(kind AlertType, confidence float64, addresses []string, attributes map[string]any) Alert {
	return Alert{ID: fmt.Sprintf("%s-%d", kind, time.Now().UnixNano()), Type: kind,
		Severity: severity(confidence), Confidence: confidence, DetectedAt: time.Now().UTC(),
		Addresses: addresses, Attributes: attributes}
}

func severity(confidence float64) string {
	switch {
	case confidence >= .8:
		return "HIGH"
	case confidence >= .5:
		return "MEDIUM"
	default:
		return "LOW"
	}
}
