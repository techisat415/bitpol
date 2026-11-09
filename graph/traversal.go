package graph

import (
	"container/heap"
	"log/slog"
	"math/big"
	"sync"
	"time"
)

type Options struct {
	StartTime        time.Time
	EndTime          time.Time
	MaxDepth         int
	MinUSDValue      float64
	MinNativeETH     float64
	PrimaryRatio     float64
	SecondaryRatio   float64
	HubThreshold     uint64
	InitialInflowUSD float64
}

func (o Options) withDefaults() Options {
	if o.MaxDepth <= 0 {
		o.MaxDepth = 8
	}
	if o.MinUSDValue <= 0 {
		o.MinUSDValue = 1000
	}
	if o.MinNativeETH <= 0 {
		o.MinNativeETH = 0.05
	}
	if o.PrimaryRatio <= 0 {
		o.PrimaryRatio = 0.8
	}
	if o.SecondaryRatio <= 0 {
		o.SecondaryRatio = 0.2
	}
	if o.HubThreshold == 0 {
		o.HubThreshold = 5000
	}
	return o
}

type PathStep struct {
	From, To, TxHash string
	Timestamp        time.Time
	USDValue         float64
	Score            float64
}

type Path struct {
	Steps        []PathStep
	Score        float64
	TerminalSink bool
}

type SinkCheck struct {
	At     string
	Edge   *Edge
	Reason string
}

type Result struct {
	Paths        []Path
	SinkChecks   []SinkCheck
	OffRampExits []string
}

type searchState struct {
	node     string
	previous time.Time
	score    float64
	inflow   float64
	depth    int
	path     []PathStep
	sequence uint64
}

type stateHeap []*searchState

func (h stateHeap) Len() int { return len(h) }
func (h stateHeap) Less(i, j int) bool {
	if h[i].score == h[j].score {
		return h[i].sequence < h[j].sequence
	}
	return h[i].score > h[j].score
}
func (h stateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *stateHeap) Push(x any)   { *h = append(*h, x.(*searchState)) }
func (h *stateHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

var statePool = sync.Pool{New: func() any { return new(searchState) }}

func acquireState() *searchState {
	state := statePool.Get().(*searchState)
	*state = searchState{score: 1}
	return state
}

func releaseState(state *searchState) {
	state.path = state.path[:0]
	statePool.Put(state)
}

// Traverse performs a score-ordered, time-causal depth-limited search.
// The graph remains available to concurrent writers; each adjacency range is
// copied under a node read lock and the search itself owns all mutable state.
func (g *Graph) Traverse(start string, options Options, logger *slog.Logger) (Result, error) {
	options = options.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	if _, ok := g.Node(start); !ok {
		return Result{}, ErrInvalidAddress
	}
	result := Result{}
	queue := &stateHeap{}
	heap.Init(queue)
	root := acquireState()
	root.node, root.previous, root.inflow = start, options.StartTime, options.InitialInflowUSD
	root.sequence = 1
	heap.Push(queue, root)
	defer func() {
		for queue.Len() > 0 {
			releaseState(heap.Pop(queue).(*searchState))
		}
	}()

	var sequence uint64 = 1
	for queue.Len() > 0 {
		current := heap.Pop(queue).(*searchState)
		if current.depth >= options.MaxDepth {
			if len(current.path) > 0 {
				result.Paths = append(result.Paths, makePath(current))
			}
			releaseState(current)
			continue
		}
		node, ok := g.Node(current.node)
		if !ok {
			releaseState(current)
			continue
		}
		_, entityType := node.Metadata()
		if isTerminal(node, entityType, options.HubThreshold) {
			if len(current.path) > 0 {
				path := makePath(current)
				path.TerminalSink = true
				result.Paths = append(result.Paths, path)
			}
			result.OffRampExits = append(result.OffRampExits, current.node)
			logger.Info("off-ramp terminal sink", "wallet", current.node,
				"degree", node.Degree(), "entity_type", entityType)
			releaseState(current)
			continue
		}
		edges := node.edgesBetween(options.StartTime, options.EndTime)
		for _, edge := range edges {
			if edge.Timestamp.Before(current.previous) {
				continue // temporal causality: delta_t < 0 is impossible.
			}
			if !passesValueThreshold(edge, options) {
				continue
			}
			class, nextScore := classify(edge.USDValue, current.inflow, current.score, options)
			if class == secondary {
				result.SinkChecks = append(result.SinkChecks, SinkCheck{
					At: current.node, Edge: edge, Reason: "secondary value slice",
				})
				continue
			}
			next := acquireState()
			next.node, next.previous = edge.ToAddress, edge.Timestamp
			next.score, next.inflow = nextScore, edge.USDValue
			next.depth, next.sequence = current.depth+1, sequence
			sequence++
			next.path = append(next.path, current.path...)
			next.path = append(next.path, PathStep{
				From: edge.FromAddress, To: edge.ToAddress, TxHash: edge.TxHash,
				Timestamp: edge.Timestamp, USDValue: edge.USDValue, Score: nextScore,
			})
			heap.Push(queue, next)
		}
		if len(current.path) > 0 && len(edges) == 0 {
			result.Paths = append(result.Paths, makePath(current))
		}
		releaseState(current)
	}
	return result, nil
}

type flowClass uint8

const (
	primary flowClass = iota
	secondary
	decayed
)

func classify(outflow, inflow, score float64, options Options) (flowClass, float64) {
	if inflow <= 0 {
		return primary, score
	}
	ratio := outflow / inflow
	switch {
	case ratio >= options.PrimaryRatio:
		return primary, score
	case ratio < options.SecondaryRatio:
		return secondary, score
	default:
		// Middle-band flow is retained but decays proportionally.
		return decayed, score * ratio
	}
}

func passesValueThreshold(edge *Edge, options Options) bool {
	if edge.USDValue >= options.MinUSDValue {
		return true
	}
	if !edge.Native || options.MinNativeETH <= 0 {
		return false
	}
	// ETH has 18 decimals. Compare as a rational to avoid float conversion
	// of the 256-bit amount.
	minWei := new(big.Float).Mul(big.NewFloat(options.MinNativeETH), big.NewFloat(1e18))
	amount := new(big.Float).SetInt(edge.RawAmount)
	return amount.Cmp(minWei) >= 0
}

func isTerminal(node *Node, entityType string, threshold uint64) bool {
	return node.Degree() > threshold ||
		entityType == EntityExchange ||
		entityType == EntityHotWallet
}

func makePath(state *searchState) Path {
	steps := append([]PathStep(nil), state.path...)
	return Path{Steps: steps, Score: state.score}
}
