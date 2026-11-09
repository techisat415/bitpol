package redisstore

import (
	"testing"
	"time"
)

func TestNeighborCacheTTL(t *testing.T) {
	cache := NeighborCache{
		HotWalletThreshold: 5000,
		HotTTL:             30 * time.Second,
		ColdTTL:            10 * time.Minute,
	}
	if got := cache.TTL(5000); got != 30*time.Second {
		t.Fatalf("hot TTL=%s", got)
	}
	if got := cache.TTL(10); got != 10*time.Minute {
		t.Fatalf("cold TTL=%s", got)
	}
}
