package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/techisat415/bitpol/internal/graph"
	"github.com/techisat415/bitpol/internal/risk"
	redisstore "github.com/techisat415/bitpol/internal/storage/redis"
)

func main() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("invalid REDIS_URL", "error", err)
		os.Exit(2)
	}
	client := redis.NewClient(options)
	defer client.Close()

	engine := risk.NewRiskEngine(graph.New(), risk.RiskConfig{})
	consumer := redisstore.NewConsumer(
		&redisstore.RedisStore{Client: client},
		engine,
		redisstore.ConsumerConfig{},
		nil,
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("graph worker stopped", "error", err)
		os.Exit(1)
	}
}
