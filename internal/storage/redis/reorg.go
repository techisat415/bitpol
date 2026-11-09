package redisstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

type BlockRef struct {
	Number      uint64
	Hash        string
	Wallets     []string
	EdgeMembers map[string][]string
}

type ReorgStore interface {
	RemoveEdges(context.Context, string, []string) error
	InvalidateWallets(context.Context, []string) error
}

type RedisReorgStore struct{ Client redis.UniversalClient }

func (s RedisReorgStore) RemoveEdges(ctx context.Context, wallet string, members []string) error {
	if len(members) == 0 {
		return nil
	}
	values := make([]interface{}, len(members))
	for i, member := range members {
		values[i] = member
	}
	return s.Client.ZRem(ctx, "edges:out:"+wallet, values...).Err()
}

func (s RedisReorgStore) InvalidateWallets(ctx context.Context, wallets []string) error {
	if len(wallets) == 0 {
		return nil
	}
	return s.Client.HDel(ctx, "wallet:risk_scores", wallets...).Err()
}

type ReorgWindow struct {
	mu     sync.Mutex
	blocks []BlockRef
	limit  int
	store  ReorgStore
}

func NewReorgWindow(store ReorgStore, limit int) *ReorgWindow {
	if limit <= 0 {
		limit = 64
	}
	return &ReorgWindow{limit: limit, store: store}
}

func (w *ReorgWindow) Add(ref BlockRef) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.blocks = append(w.blocks, ref)
	if len(w.blocks) > w.limit {
		w.blocks = w.blocks[len(w.blocks)-w.limit:]
	}
}

func (w *ReorgWindow) Reconcile(ctx context.Context, canonical BlockRef) ([]BlockRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	index := -1
	for i := len(w.blocks) - 1; i >= 0; i-- {
		if w.blocks[i].Number == canonical.Number && w.blocks[i].Hash == canonical.Hash {
			index = i
			break
		}
	}
	if index >= 0 {
		return nil, nil
	}
	orphaned := make([]BlockRef, 0)
	for _, block := range w.blocks {
		if block.Number >= canonical.Number {
			orphaned = append(orphaned, block)
		}
	}
	var wallets []string
	for _, block := range orphaned {
		for wallet, members := range block.EdgeMembers {
			if err := w.store.RemoveEdges(ctx, wallet, members); err != nil {
				return nil, fmt.Errorf("remove orphaned block %d: %w", block.Number, err)
			}
		}
		wallets = append(wallets, block.Wallets...)
	}
	if err := w.store.InvalidateWallets(ctx, wallets); err != nil {
		return nil, fmt.Errorf("invalidate reorg wallets: %w", err)
	}
	w.blocks = w.blocks[:0]
	return orphaned, nil
}
