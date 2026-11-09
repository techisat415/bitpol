package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
)

const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

type transfer struct {
	From, To, Token, TxHash string
	Amount                  string
	Block                   uint64
	Timestamp               time.Time
}

type engine struct {
	rdb     *redis.Client
	workers int
}

func main() {
	rpcURL := os.Getenv("ETH_WSS_URL")
	if rpcURL == "" {
		slog.Error("ETH_WSS_URL is required")
		os.Exit(2)
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("invalid REDIS_URL", "error", err)
		os.Exit(2)
	}
	rdb := redis.NewClient(options)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, rpcURL, rdb, 8); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("ingest stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rpcURL string, rdb *redis.Client, workers int) error {
	defer rdb.Close()
	for {
		err := ingestConnection(ctx, rpcURL, rdb, workers)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("websocket connection ended; reconnecting", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func ingestConnection(ctx context.Context, rpcURL string, rdb *redis.Client, workerCount int) error {
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("dial ethereum: %w", err)
	}
	defer client.Close()
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("get chain ID: %w", err)
	}
	headers := make(chan *types.Header, workerCount*2)
	sub, err := client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("subscribe heads: %w", err)
	}
	defer sub.Unsubscribe()

	jobs := make(chan *types.Header, workerCount)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for header := range jobs {
				if err := processBlock(ctx, client, rdb, chainID, header); err != nil {
					slog.Error("process block failed", "block", header.Number, "error", err)
				}
			}
		}()
	}
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return err
		case header := <-headers:
			select {
			case jobs <- header:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func processBlock(ctx context.Context, client *ethclient.Client, rdb *redis.Client, chainID *big.Int, header *types.Header) error {
	blockKey := fmt.Sprintf("canonical:block:%d", header.Number.Uint64())
	existing, err := rdb.Get(ctx, blockKey).Result()
	if err == nil {
		if existing == header.Hash().Hex() {
			return nil
		}
		if _, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "stream:reorgs",
			Values: map[string]any{"block_number": header.Number.Uint64(), "old_hash": existing,
				"new_hash": header.Hash().Hex()}}).Result(); err != nil {
			return fmt.Errorf("publish reorg: %w", err)
		}
	} else if err != redis.Nil {
		return fmt.Errorf("check canonical block: %w", err)
	}
	if err := rdb.Set(ctx, blockKey, header.Hash().Hex(), 0).Err(); err != nil {
		return fmt.Errorf("mark canonical block: %w", err)
	}
	block, err := client.BlockByHash(ctx, header.Hash())
	if err != nil {
		return fmt.Errorf("get block: %w", err)
	}
	transfers := make([]transfer, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		if tx.Value().Sign() > 0 && tx.To() != nil {
			from, err := types.Sender(types.LatestSignerForChainID(chainID), tx)
			if err != nil {
				slog.Warn("cannot recover native sender", "tx", tx.Hash(), "error", err)
			} else {
				transfers = append(transfers, transfer{
					From: from.Hex(), To: tx.To().Hex(), Token: zeroAddress,
					TxHash: tx.Hash().Hex(), Amount: tx.Value().String(), Block: block.NumberU64(),
					Timestamp: time.Unix(int64(block.Time()), 0).UTC(),
				})
			}
		}
	}
	blockHash := header.Hash()
	logs, err := client.FilterLogs(ctx, ethereum.FilterQuery{
		BlockHash: &blockHash,
		Topics:    [][]common.Hash{{crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))}},
	})
	if err != nil {
		return fmt.Errorf("filter logs: %w", err)
	}
	for _, entry := range logs {
		if len(entry.Topics) != 3 || entry.Topics[0].Hex() != transferTopic {
			continue
		}
		transfers = append(transfers, transfer{
			From:  common.BytesToAddress(entry.Topics[1].Bytes()).Hex(),
			To:    common.BytesToAddress(entry.Topics[2].Bytes()).Hex(),
			Token: entry.Address.Hex(), TxHash: entry.TxHash.Hex(),
			Amount: new(big.Int).SetBytes(entry.Data).String(), Block: block.NumberU64(),
			Timestamp: time.Unix(int64(block.Time()), 0).UTC(),
		})
	}
	pipe := rdb.Pipeline()
	for _, item := range transfers {
		member := strings.Join([]string{item.To, item.Amount, item.TxHash, item.Token}, ":")
		pipe.ZAdd(ctx, "edges:out:"+strings.ToLower(item.From), redis.Z{
			Score: float64(item.Block), Member: member,
		})
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream:touched_wallets", Values: map[string]any{
				"wallet_from": item.From, "wallet_to": item.To,
				"block_number": item.Block, "timestamp": item.Timestamp.Unix(),
				"tx_hash": item.TxHash, "block_hash": header.Hash().Hex(),
			},
		})
	}
	if len(transfers) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("redis pipeline: %w", err)
		}
	}
	return nil
}

const zeroAddress = "0x0000000000000000000000000000000000000000"
