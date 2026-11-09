package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	SanctionsKey  = "sanctions:sdn"
	RiskScoresKey = "wallet:risk_scores"
	DegreesKey    = "wallet:degrees"
	TouchStream   = "stream:touched_wallets"
)

type RedisStore struct {
	Client redis.UniversalClient
	Window time.Duration
}

type NeighborCache struct {
	Client             redis.UniversalClient
	HotWalletThreshold uint64
	HotTTL             time.Duration
	ColdTTL            time.Duration
}

func (c NeighborCache) ttl(outDegree uint64) time.Duration {
	if outDegree >= c.HotWalletThreshold {
		if c.HotTTL > 0 {
			return c.HotTTL
		}
		return 30 * time.Second
	}
	if c.ColdTTL > 0 {
		return c.ColdTTL
	}
	return 10 * time.Minute
}

func (c NeighborCache) TTL(outDegree uint64) time.Duration {
	return c.ttl(outDegree)
}

func (c NeighborCache) Set(ctx context.Context, wallet string, neighbors []string, outDegree uint64) error {
	if wallet == "" {
		return fmt.Errorf("wallet is required")
	}
	payload, err := json.Marshal(neighbors)
	if err != nil {
		return fmt.Errorf("marshal neighbors: %w", err)
	}
	return c.Client.Set(ctx, "neighbors:"+wallet, payload, c.ttl(outDegree)).Err()
}

func (c NeighborCache) Get(ctx context.Context, wallet string) ([]string, error) {
	payload, err := c.Client.Get(ctx, "neighbors:"+wallet).Bytes()
	if err != nil {
		return nil, err
	}
	var neighbors []string
	if err := json.Unmarshal(payload, &neighbors); err != nil {
		return nil, fmt.Errorf("decode neighbors: %w", err)
	}
	return neighbors, nil
}

func (s *RedisStore) riskKey(wallet string) string { return wallet }
func EdgeWindowKey(wallet string) string           { return "edges:out:" + wallet }

func (s *RedisStore) IsSanctioned(ctx context.Context, wallet string) (bool, error) {
	return s.Client.SIsMember(ctx, SanctionsKey, wallet).Result()
}

func (s *RedisStore) SetRisk(ctx context.Context, wallet string, score float64) error {
	if score < 0 || score > 1 {
		return fmt.Errorf("risk score out of range: %f", score)
	}
	return s.Client.HSet(ctx, RiskScoresKey, s.riskKey(wallet), strconv.FormatFloat(score, 'g', 17, 64)).Err()
}

func (s *RedisStore) InvalidateRisk(ctx context.Context, wallets ...string) error {
	if len(wallets) == 0 {
		return nil
	}
	return s.Client.HDel(ctx, RiskScoresKey, wallets...).Err()
}

func (s *RedisStore) Risk(ctx context.Context, wallet string) (float64, error) {
	value, err := s.Client.HGet(ctx, RiskScoresKey, wallet).Result()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(value, 64)
}

type TouchEvent struct {
	WalletFrom   string
	WalletTo     string
	BlockNumber  uint64
	Timestamp    time.Time
	AmountRaw    string
	TxHash       string
	TokenAddress string
}

func (s *RedisStore) ApplyTouch(ctx context.Context, event TouchEvent) error {
	if event.WalletFrom == "" || event.WalletTo == "" || event.TxHash == "" {
		return fmt.Errorf("touch event missing wallet or transaction identity")
	}
	member := event.WalletTo + ":" + event.AmountRaw + ":" + event.TxHash + ":" + event.TokenAddress
	score := float64(event.Timestamp.Unix())
	windowKey := EdgeWindowKey(event.WalletFrom)
	_, err := s.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZAdd(ctx, windowKey, redis.Z{Score: score, Member: member})
		if s.Window > 0 {
			pipe.ZRemRangeByScore(ctx, windowKey, "-inf", strconv.FormatInt(time.Now().Add(-s.Window).Unix(), 10))
		}
		pipe.HIncrBy(ctx, DegreesKey, event.WalletFrom+":out_degree", 1)
		pipe.HIncrBy(ctx, DegreesKey, event.WalletTo+":in_degree", 1)
		pipe.HDel(ctx, RiskScoresKey, event.WalletFrom, event.WalletTo)
		return nil
	})
	return err
}

func (s *RedisStore) TouchFromMessage(message redis.XMessage) (TouchEvent, error) {
	get := func(name string) (string, error) {
		value, ok := message.Values[name]
		if !ok {
			return "", fmt.Errorf("stream message missing %q", name)
		}
		return fmt.Sprint(value), nil
	}
	from, err := get("wallet_from")
	if err != nil {
		return TouchEvent{}, err
	}
	to, err := get("wallet_to")
	if err != nil {
		return TouchEvent{}, err
	}
	txHash, err := get("tx_hash")
	if err != nil {
		return TouchEvent{}, err
	}
	timestamp, err := get("timestamp")
	if err != nil {
		return TouchEvent{}, err
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return TouchEvent{}, fmt.Errorf("invalid timestamp: %w", err)
	}
	amount, _ := get("amount_raw")
	token, _ := get("token_address")
	block, _ := get("block_number")
	blockNumber, _ := strconv.ParseUint(block, 10, 64)
	return TouchEvent{WalletFrom: from, WalletTo: to, TxHash: txHash,
		Timestamp: time.Unix(seconds, 0), AmountRaw: amount, TokenAddress: token,
		BlockNumber: blockNumber}, nil
}

func NewRedisClient(options *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(options)
}
