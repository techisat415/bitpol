package redisstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/techisat415/bitpol/internal/risk"
)

type ConsumerConfig struct {
	Group          string
	ConsumerPrefix string
	Workers        int
	BatchSize      int64
	ReadBlock      time.Duration
	ReevaluateHops int
}

func (c ConsumerConfig) defaults() ConsumerConfig {
	if c.Group == "" {
		c.Group = "risk-engine"
	}
	if c.ConsumerPrefix == "" {
		c.ConsumerPrefix = "risk-worker"
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 32
	}
	if c.ReadBlock <= 0 {
		c.ReadBlock = 5 * time.Second
	}
	if c.ReevaluateHops <= 0 {
		c.ReevaluateHops = 3
	}
	return c
}

type Consumer struct {
	store  *RedisStore
	engine *risk.RiskEngine
	config ConsumerConfig
	logger *slog.Logger
}

func NewConsumer(store *RedisStore, engine *risk.RiskEngine, config ConsumerConfig, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{store: store, engine: engine, config: config.defaults(), logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.store == nil || c.store.Client == nil || c.engine == nil {
		return errors.New("consumer requires Redis store and risk engine")
	}
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, c.config.Workers)
	var workers sync.WaitGroup
	for i := 0; i < c.config.Workers; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			errs <- c.worker(workerContext, fmt.Sprintf("%s-%d", c.config.ConsumerPrefix, index))
		}(i)
	}
	var firstErr error
	select {
	case <-ctx.Done():
		firstErr = ctx.Err()
	case err := <-errs:
		firstErr = err
	}
	cancel()
	workers.Wait()
	if firstErr != nil && !errors.Is(firstErr, context.Canceled) {
		return firstErr
	}
	return ctx.Err()
}

func (c *Consumer) ensureGroup(ctx context.Context) error {
	err := c.store.Client.XGroupCreateMkStream(ctx, TouchStream, c.config.Group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

func (c *Consumer) worker(ctx context.Context, consumer string) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		messages, err := c.store.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: c.config.Group, Consumer: consumer, Streams: []string{TouchStream, ">"},
			Count: c.config.BatchSize, Block: c.config.ReadBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return fmt.Errorf("read stream as %s: %w", consumer, err)
		}
		for _, stream := range messages {
			for _, message := range stream.Messages {
				if err := c.handle(ctx, message); err != nil {
					c.logger.Error("touch event failed", "id", message.ID, "error", err)
					continue
				}
				if err := c.store.Client.XAck(ctx, TouchStream, c.config.Group, message.ID).Err(); err != nil {
					return fmt.Errorf("ack %s: %w", message.ID, err)
				}
			}
		}
	}
}

func (c *Consumer) handle(ctx context.Context, message redis.XMessage) error {
	event, err := c.store.TouchFromMessage(message)
	if err != nil {
		return err
	}
	if err := c.store.ApplyTouch(ctx, event); err != nil {
		return err
	}
	return c.reevaluate(ctx, event.WalletTo, c.config.ReevaluateHops)
}

func (c *Consumer) reevaluate(ctx context.Context, root string, hops int) error {
	frontier := []string{root}
	seen := map[string]bool{root: true}
	for depth := 0; depth <= hops && len(frontier) > 0; depth++ {
		next := make([]string, 0)
		for _, wallet := range frontier {
			score, err := c.engine.Score(wallet)
			if err != nil {
				continue
			}
			if err := c.store.SetRisk(ctx, wallet, score); err != nil {
				return err
			}
			edges, _ := c.engine.Graph().Outgoing(wallet, time.Time{}, time.Now())
			for _, edge := range edges {
				if !seen[edge.ToAddress] {
					seen[edge.ToAddress] = true
					next = append(next, edge.ToAddress)
				}
			}
		}
		frontier = next
	}
	return nil
}

func ParseStreamID(id string) (uint64, uint64, error) {
	parts := [2]uint64{}
	var err error
	if _, scanErr := fmt.Sscanf(id, "%d-%d", &parts[0], &parts[1]); scanErr != nil {
		err = scanErr
	}
	if err != nil || id == "" {
		return 0, 0, fmt.Errorf("invalid Redis stream ID %q", id)
	}
	return parts[0], parts[1], nil
}

func StreamIDAfter(left, right string) (bool, error) {
	leftMS, leftSeq, err := ParseStreamID(left)
	if err != nil {
		return false, err
	}
	rightMS, rightSeq, err := ParseStreamID(right)
	if err != nil {
		return false, err
	}
	return leftMS > rightMS || leftMS == rightMS && leftSeq > rightSeq, nil
}
