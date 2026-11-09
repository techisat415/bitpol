package typologies

import (
	"encoding/json"
	"time"
)

type AlertType string

const (
	AlertPeeling  AlertType = "PEELING_CHAIN"
	AlertFanRing  AlertType = "FAN_OUT_IN_RING"
	AlertMixer    AlertType = "MIXER_CORRELATION"
	AlertBackward AlertType = "BACKWARD_TRACE"
)

// Alert is intentionally stable and JSON-friendly for Redis Streams, review
// queues, and PostgreSQL JSONB persistence.
type Alert struct {
	ID         string         `json:"id"`
	Type       AlertType      `json:"type"`
	Severity   string         `json:"severity"`
	Confidence float64        `json:"confidence"`
	DetectedAt time.Time      `json:"detected_at"`
	Addresses  []string       `json:"addresses"`
	Edges      []EdgeRef      `json:"edges,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type EdgeRef struct {
	From      string    `json:"from"`
	To        string    `json:"to"`
	TxHash    string    `json:"tx_hash,omitempty"`
	Token     string    `json:"token"`
	USDValue  float64   `json:"usd_value"`
	Timestamp time.Time `json:"timestamp"`
}

func (a Alert) JSON() ([]byte, error) { return json.Marshal(a) }
