// Package graph contains the in-memory dynamic multigraph and bounded traversal
// implementation used by the Phase 2 analysis workers.
package graph

import (
	"errors"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	EntityExchange  = "EXCHANGE"
	EntityMixer     = "MIXER"
	EntityUntagged  = "UNTAGGED"
	EntityHotWallet = "IS_HOT_WALLET"
)

var (
	ErrInvalidAddress = errors.New("address must be a non-empty canonical identifier")
	ErrInvalidAmount  = errors.New("raw amount must be non-negative")
)

// Edge is immutable after insertion. RawAmount is copied on insertion, so
// callers may safely reuse their input big.Int.
type Edge struct {
	FromAddress  string
	ToAddress    string
	TokenAddress string
	RawAmount    *big.Int
	USDValue     float64
	Timestamp    time.Time
	TxHash       string
	Native       bool
}

type Node struct {
	Address    string
	KYCStatus  string
	EntityType string

	inDegree  atomic.Uint64
	outDegree atomic.Uint64
	mu        sync.RWMutex
	outgoing  []*Edge
	incoming  []*Edge
}

func (n *Node) InDegree() uint64  { return n.inDegree.Load() }
func (n *Node) OutDegree() uint64 { return n.outDegree.Load() }
func (n *Node) Degree() uint64    { return n.InDegree() + n.OutDegree() }

func (n *Node) Metadata() (kycStatus, entityType string) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.KYCStatus, n.EntityType
}

func (n *Node) edgesBetween(start, end time.Time) []*Edge {
	n.mu.RLock()
	defer n.mu.RUnlock()
	first := sort.Search(len(n.outgoing), func(i int) bool {
		return !n.outgoing[i].Timestamp.Before(start)
	})
	last := sort.Search(len(n.outgoing), func(i int) bool {
		return n.outgoing[i].Timestamp.After(end)
	})
	if first >= last {
		return nil
	}
	// Copy only the slice header/backing references; Edge values remain immutable.
	return append([]*Edge(nil), n.outgoing[first:last]...)
}

func (n *Node) incomingBetween(start, end time.Time) []*Edge {
	n.mu.RLock()
	defer n.mu.RUnlock()
	first := sort.Search(len(n.incoming), func(i int) bool {
		return !n.incoming[i].Timestamp.Before(start)
	})
	last := sort.Search(len(n.incoming), func(i int) bool {
		return n.incoming[i].Timestamp.After(end)
	})
	if first >= last {
		return nil
	}
	return append([]*Edge(nil), n.incoming[first:last]...)
}

type Graph struct {
	nodes sync.Map // map[string]*Node
}

func New() *Graph { return &Graph{} }

func (g *Graph) GetOrCreateNode(address string) (*Node, error) {
	if address == "" {
		return nil, ErrInvalidAddress
	}
	actual, _ := g.nodes.LoadOrStore(address, &Node{
		Address: address, KYCStatus: "UNKNOWN", EntityType: EntityUntagged,
	})
	return actual.(*Node), nil
}

func (g *Graph) SetMetadata(address, kycStatus, entityType string) error {
	node, err := g.GetOrCreateNode(address)
	if err != nil {
		return err
	}
	node.mu.Lock()
	node.KYCStatus, node.EntityType = kycStatus, entityType
	node.mu.Unlock()
	return nil
}

func (g *Graph) AddEdge(input Edge) error {
	if input.FromAddress == "" || input.ToAddress == "" || input.TokenAddress == "" {
		return ErrInvalidAddress
	}
	if input.RawAmount == nil || input.RawAmount.Sign() < 0 {
		return ErrInvalidAmount
	}
	if input.USDValue < 0 {
		return errors.New("USD value must be non-negative")
	}
	raw := new(big.Int).Set(input.RawAmount)
	edge := input
	edge.RawAmount = raw
	from, err := g.GetOrCreateNode(input.FromAddress)
	if err != nil {
		return err
	}
	if _, err := g.GetOrCreateNode(input.ToAddress); err != nil {
		return err
	}
	from.mu.Lock()
	position := sort.Search(len(from.outgoing), func(i int) bool {
		return !from.outgoing[i].Timestamp.Before(edge.Timestamp)
	})
	from.outgoing = append(from.outgoing, nil)
	copy(from.outgoing[position+1:], from.outgoing[position:])
	from.outgoing[position] = &edge
	from.mu.Unlock()
	from.outDegree.Add(1)
	to, _ := g.GetOrCreateNode(input.ToAddress)
	to.mu.Lock()
	inPosition := sort.Search(len(to.incoming), func(i int) bool {
		return !to.incoming[i].Timestamp.Before(edge.Timestamp)
	})
	to.incoming = append(to.incoming, nil)
	copy(to.incoming[inPosition+1:], to.incoming[inPosition:])
	to.incoming[inPosition] = &edge
	to.mu.Unlock()
	to.inDegree.Add(1)
	return nil
}

func (g *Graph) Node(address string) (*Node, bool) {
	node, ok := g.nodes.Load(address)
	if !ok {
		return nil, false
	}
	return node.(*Node), true
}

func (g *Graph) Outgoing(address string, start, end time.Time) ([]*Edge, bool) {
	node, ok := g.Node(address)
	if !ok {
		return nil, false
	}
	return node.edgesBetween(start, end), true
}

func (g *Graph) Incoming(address string, start, end time.Time) ([]*Edge, bool) {
	node, ok := g.Node(address)
	if !ok {
		return nil, false
	}
	return node.incomingBetween(start, end), true
}
