package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/techisat415/bitpol/internal/graph"
	"github.com/techisat415/bitpol/internal/metrics"
	"github.com/techisat415/bitpol/internal/risk"
	redisstore "github.com/techisat415/bitpol/internal/storage/redis"
	"github.com/techisat415/bitpol/internal/typologies"
)

type API struct {
	Engine  *risk.RiskEngine
	Store   *redisstore.RedisStore
	Metrics *metrics.Metrics
}

type RiskResponse struct {
	Wallet     string  `json:"wallet"`
	Score      float64 `json:"score"`
	Intrinsic  float64 `json:"intrinsic"`
	Sanctioned bool    `json:"sanctioned"`
}

type TraceResponse struct {
	Wallet       string       `json:"wallet"`
	Paths        []graph.Path `json:"paths"`
	OffRampExits []string     `json:"off_ramp_exits"`
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/wallet/", a.risk)
	mux.HandleFunc("/v1/trace/lineage", a.trace)
	mux.HandleFunc("/v1/trace/cycles", a.cycles)
	if a.Metrics != nil {
		mux.Handle("/metrics", metrics.Handler(prometheus.DefaultGatherer))
	}
	return mux
}

func (a *API) cycles(w http.ResponseWriter, r *http.Request) {
	wallet := r.URL.Query().Get("wallet")
	if wallet == "" || a.Engine == nil {
		http.Error(w, "wallet is required", http.StatusBadRequest)
		return
	}
	start := time.Now()
	rings, err := typologies.DetectRings(a.Engine.Graph(), wallet, typologies.RingConfig{
		Start: time.Unix(0, 0), End: time.Now(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if a.Metrics != nil {
		a.Metrics.TraversalDuration.WithLabelValues("cycle").Observe(time.Since(start).Seconds())
	}
	writeJSON(w, map[string]any{"wallet": wallet, "cycles": rings})
}

func (a *API) risk(w http.ResponseWriter, r *http.Request) {
	wallet := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/wallet/"), "/risk")
	if wallet == "" || a.Engine == nil {
		http.Error(w, "wallet is required", http.StatusBadRequest)
		return
	}
	start := time.Now()
	score, err := a.Engine.Score(wallet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	sanctioned := false
	if a.Store != nil {
		sanctioned, err = a.Store.IsSanctioned(r.Context(), wallet)
		if err != nil {
			http.Error(w, "sanctions lookup failed", http.StatusBadGateway)
			return
		}
	}
	if a.Metrics != nil {
		a.Metrics.TraversalDuration.WithLabelValues("forward").Observe(time.Since(start).Seconds())
	}
	writeJSON(w, RiskResponse{Wallet: wallet, Score: score, Intrinsic: a.Engine.IntrinsicRisk(wallet), Sanctioned: sanctioned})
}

func (a *API) trace(w http.ResponseWriter, r *http.Request) {
	wallet := r.URL.Query().Get("wallet")
	if wallet == "" || a.Engine == nil {
		http.Error(w, "wallet is required", http.StatusBadRequest)
		return
	}
	start := time.Now()
	maxHops := 0
	if raw := r.URL.Query().Get("max_hops"); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 {
			http.Error(w, "max_hops must be a positive integer", http.StatusBadRequest)
			return
		}
		maxHops = parsed
	}
	options := graph.Options{
		StartTime: time.Unix(0, 0), EndTime: time.Now(),
		MinUSDValue: 0.01,
	}
	var result graph.Result
	var err error
	if maxHops > 0 {
		result, err = a.Engine.Graph().TraverseWithHops(wallet, options, maxHops, nil)
	} else {
		result, err = a.Engine.Graph().Traverse(wallet, options, nil)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if a.Metrics != nil {
		a.Metrics.TraversalDuration.WithLabelValues("forward").Observe(time.Since(start).Seconds())
	}
	writeJSON(w, TraceResponse{Wallet: wallet, Paths: result.Paths, OffRampExits: result.OffRampExits})
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

type Alert struct {
	Wallet   string  `json:"wallet"`
	Typology string  `json:"typology"`
	Score    float64 `json:"score"`
	Reason   string  `json:"reason"`
}

type WebhookDispatcher struct {
	Client    *http.Client
	URL       string
	Threshold float64
	Queue     chan Alert
	wg        sync.WaitGroup
}

func NewWebhookDispatcher(url string, workers, queueSize int) *WebhookDispatcher {
	if workers <= 0 {
		workers = 2
	}
	if queueSize <= 0 {
		queueSize = 1024
	}
	d := &WebhookDispatcher{Client: &http.Client{Timeout: 5 * time.Second}, URL: url,
		Threshold: .85, Queue: make(chan Alert, queueSize)}
	d.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go d.worker()
	}
	return d
}

func (d *WebhookDispatcher) Enqueue(alert Alert) bool {
	if alert.Score < d.Threshold {
		return false
	}
	select {
	case d.Queue <- alert:
		return true
	default:
		return false
	}
}

func (d *WebhookDispatcher) worker() {
	defer d.wg.Done()
	for alert := range d.Queue {
		body, err := json.Marshal(alert)
		if err != nil {
			continue
		}
		req, err := http.NewRequest(http.MethodPost, d.URL, strings.NewReader(string(body)))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.Client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}
}

func (d *WebhookDispatcher) Close() {
	close(d.Queue)
	d.wg.Wait()
}

func (a Alert) Validate() error {
	if a.Wallet == "" || a.Typology == "" {
		return fmt.Errorf("wallet and typology are required")
	}
	if a.Score < 0 || a.Score > 1 {
		return fmt.Errorf("score must be between 0 and 1")
	}
	return nil
}
