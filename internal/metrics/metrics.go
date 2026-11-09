package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type Metrics struct {
	IngestBlocks      *prometheus.CounterVec
	TraversalDuration *prometheus.HistogramVec
	RedisHits         prometheus.Counter
	RedisMisses       prometheus.Counter
	FlaggedLineages   *prometheus.CounterVec
}

func Handler(registerer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(registerer, promhttp.HandlerOpts{})
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		IngestBlocks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "engine_ingest_blocks_total", Help: "Blocks accepted by ingestion.",
		}, nil),
		TraversalDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "engine_traversal_duration_seconds", Help: "Traversal latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"traversal_type"}),
		RedisHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_redis_cache_hits_total", Help: "Redis cache hits.",
		}),
		RedisMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_redis_cache_misses_total", Help: "Redis cache misses.",
		}),
		FlaggedLineages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "engine_flagged_lineages_detected_total", Help: "Detected flagged lineages.",
		}, []string{"typology"}),
	}
	registerer.MustRegister(m.IngestBlocks, m.TraversalDuration, m.RedisHits, m.RedisMisses, m.FlaggedLineages)
	return m
}
