package redisstore

import (
	"context"
	"testing"
)

type fakeReorgStore struct {
	removed     int
	invalidated int
}

func (s *fakeReorgStore) RemoveEdges(_ context.Context, _ string, members []string) error {
	s.removed += len(members)
	return nil
}

func (s *fakeReorgStore) InvalidateWallets(_ context.Context, wallets []string) error {
	s.invalidated += len(wallets)
	return nil
}

func TestReorgWindowRollsBackOrphanedBlocks(t *testing.T) {
	store := &fakeReorgStore{}
	window := NewReorgWindow(store, 64)
	window.Add(BlockRef{Number: 10, Hash: "old", Wallets: []string{"a"},
		EdgeMembers: map[string][]string{"a": {"b:1:tx:token"}}})
	orphaned, err := window.Reconcile(context.Background(), BlockRef{Number: 10, Hash: "new"})
	if err != nil || len(orphaned) != 1 {
		t.Fatalf("orphaned=%+v err=%v", orphaned, err)
	}

	if store.removed != 1 || store.invalidated != 1 {
		t.Fatalf("rollback counts removed=%d invalidated=%d", store.removed, store.invalidated)
	}
}

func TestStreamOffsets(t *testing.T) {
	ok, err := StreamIDAfter("100-2", "100-1")
	if err != nil || !ok {
		t.Fatalf("offset comparison failed: %v %v", ok, err)
	}
	ok, err = StreamIDAfter("100-1", "100-1")
	if err != nil || ok {
		t.Fatalf("equal offsets failed: %v %v", ok, err)
	}
	if _, _, err := ParseStreamID("invalid"); err == nil {
		t.Fatal("expected invalid ID")
	}
}
