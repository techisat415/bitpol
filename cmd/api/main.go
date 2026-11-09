package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/techisat415/bitpol/internal/api"
	"github.com/techisat415/bitpol/internal/graph"
	"github.com/techisat415/bitpol/internal/metrics"
	"github.com/techisat415/bitpol/internal/risk"
	redisstore "github.com/techisat415/bitpol/internal/storage/redis"
	"strings"
)

func main() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://redis:6379/0"
	}
	var client redis.UniversalClient
	if addresses := os.Getenv("REDIS_CLUSTER_ADDRS"); addresses != "" {
		client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addresses, ",")})
	} else {
		options, err := redis.ParseURL(redisURL)
		if err != nil {
			slog.Error("invalid REDIS_URL", "error", err)
			os.Exit(2)
		}
		client = redis.NewClient(options)
	}
	defer client.Close()
	g := graph.New()
	engine := risk.NewRiskEngine(g, risk.RiskConfig{})
	engineMetrics := metrics.NewMetrics(prometheus.DefaultRegisterer)
	server := &http.Server{Addr: ":8080", Handler: (&api.API{
		Engine:  engine,
		Store:   &redisstore.RedisStore{Client: client, Window: 24 * time.Hour},
		Metrics: engineMetrics,
	}).Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	slog.Info("compliance API listening", "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
