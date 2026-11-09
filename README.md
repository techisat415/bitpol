# Bitpol

Phase 1 Crypto Financial Contagion & Fraud Ring Traversal Engine.

## Components

- [`internal/storage/postgres/001_schema.sql`]: PostgreSQL 16 schema. `edges` is range-partitioned by block number and stores `raw_amount` as `NUMERIC(78,0)`, never as a floating-point value.
- [`internal/storage/postgres/bigquery_to_parquet.py`]: bounded BigQuery extraction of native and ERC-20 transfers into compressed Parquet batches.
- [`cmd/ingest/main.go`]: reconnecting WebSocket header subscriber, concurrent block workers, native/ERC-20 parsing, Redis pipelining, and canonical block/reorg markers.
- [`cmd/seed/main.go`]: idempotent PostgreSQL entity upsert plus Redis sanctions set loading from an operational CSV.
- [`internal/graph/`]: Phase 2 thread-safe temporal multigraph and score-ordered dual-trunk traversal.

## Phase 2 traversal

`graph.Graph` keeps immutable edge payloads in per-wallet timestamp-sorted adjacency
lists. Writers lock only the affected node; readers copy the requested adjacency
range under `RLock`, while degree counters are atomic. `raw_amount` remains a
`big.Int` copy and is never converted to a floating-point value for storage.

The traversal worker uses a pooled priority-queue state:

```text
pop highest S_path
if depth == MaxDepth: emit path
if degree(wallet) > K or entity is EXCHANGE/IS_HOT_WALLET:
    emit terminal sink/off-ramp and do not expand
for edge in [StartTime, EndTime]:
    reject when edge.timestamp < previous_edge.timestamp
    reject when USD(edge) < V_min and (not native or ETH(edge) < 0.05)
    ratio = V_out / V_in
    ratio >= 0.80: S_next = S_path
    ratio <  0.20: enqueue SinkCheck, do not expand
    otherwise:     S_next = S_path * ratio
    push destination with previous_edge.timestamp = edge.timestamp
```

The temporal condition is `Delta t = t_edge - t_previous >= 0`. The
`InitialInflowUSD` option supplies `V_in` for the root split; subsequent states
use the previous edge's value. Default `V_min` is `$1,000` or `0.05 ETH` for
native edges. Search state objects are returned to `sync.Pool` after expansion,
and callers receive copied result paths.

## Historical backfill

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r internal/storage/postgres/requirements.txt
python internal/storage/postgres/bigquery_to_parquet.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --start-block 19000000 --end-block 19010000 \
  --output-dir ./parquet
```

The resulting Parquet files can be loaded with PostgreSQL `COPY` through a staging table. ERC-20 decimals are intentionally `0` because the public transfer table does not provide authoritative decimals; resolve token metadata in a separate metadata load before presenting human units. Base-unit precision remains exact.

## Live ingestion

```bash
export ETH_WSS_URL=wss://your-ethereum-provider.example
export REDIS_URL=redis://localhost:6379/0
go run ./cmd/ingest
```

Each transfer is written to `edges:out:<from_address>` and announced on `stream:touched_wallets`. Redis keys `canonical:block:<number>` detect duplicate delivery and emit replacement headers to `stream:reorgs`. Production consumers should reconcile provisional transfers when consuming a reorg event; PostgreSQL loads should only accept finalized blocks.

## Entity registry

Create a CSV from the current OFAC SDN export and curated protocol addresses:

```text
address,entity_name,category
0x0000000000000000000000000000000000000000,example,OTHER
```

Then run:

```bash
export DATABASE_URL='postgres://...'
export REDIS_URL=redis://localhost:6379/0
export ENTITIES_FILE=/secure/config/entities.csv
go run ./cmd/seed
```

Rows categorized `SANCTIONED_OFAC` or `MIXER` are also added to `sanctions:sdn` for constant-time Redis membership checks. The SQL category constraint rejects unrecognized classifications.

## Validation

```bash
go test ./...
git diff --check
```

## Phase 3 detection workers

The asynchronous detection layer provides four independent algorithms:

- **Peeling chains** require repeated `InDegree == 1` and `OutDegree == 2`
  nodes. One output must retain at least 80% of the incoming USD value and
  the other must be below 20%; short chains are rejected.
- **Fan-out/fan-in and rings** identify branches that reconverge inside a
  bounded time window and time-causal cycles using depth-limited DFS.
- **Mixer correlation** treats configured deposit contracts as sinks and
  withdrawal contracts as sources. Matching token, denomination, timing,
  gas-price, and relayer signals produce probabilistic virtual edges only
  above the configured confidence threshold.
- **Backward tracing** walks incoming edges from a sink while enforcing
  `t_in <= t_out`, returning upstream source lineages and aggregate USD value.

All detector outputs can be wrapped in the stable alert envelope:

```json
{
  "id": "MIXER_CORRELATION-1730000000000000000",
  "type": "MIXER_CORRELATION",
  "severity": "MEDIUM",
  "confidence": 0.65,
  "detected_at": "2026-11-09T12:00:00Z",
  "addresses": ["deposit", "withdrawal"],
  "attributes": {
    "virtual_edge": true,
    "reasons": ["denomination", "token", "temporal_window"]
  },
  "edges": []
}
```

The envelope is suitable for Redis Streams or PostgreSQL JSONB storage.

## Phase 4 risk caching

The risk engine evaluates intrinsic sanctions or flagged-origin risk, incoming
network contamination, logarithmic source-degree damping, and exponential
time decay. Redis stores sanctions in `sanctions:sdn`, scores in the
`wallet:risk_scores` hash, degrees in `wallet:degrees`, and temporal outgoing
edges in `edges:out:<wallet>`.

The stream consumer uses the `risk-engine` consumer group on
`stream:touched_wallets`. Each worker atomically updates the edge window and
degree counters with a Redis transaction, invalidates affected scores, then
recomputes the touched destination and up to three downstream hops. Messages
are acknowledged only after successful processing, so failed work remains
pending for operational recovery.

## Phase 5 operations

The Phase 5 package provides synthetic peeling, fan-out/fan-in, and cycle load
generators; a 64-block reorg window with Redis edge rollback and score
invalidation; REST risk and lineage endpoints; webhook dispatch at a
configurable risk threshold; and Prometheus counters/histograms. API contracts
are provided in OpenAPI and Protobuf form. The local Compose stack starts
PostgreSQL, Redis, Prometheus, and the compliance API.

The benchmark suite records per-request latencies and throughput. Sort its
latency samples to obtain P50/P95/P99; the regression test enforces the
sub-50ms P99 target on the synthetic traversal workload. Prometheus exposes
ingestion, traversal, cache, and typology counters for production alerting.

Johnson cycle detection operates on candidate subgraphs exposed through the
cycle endpoint. It first finds strongly connected components, then enumerates
elementary cycles with blocked/unblocked sets in `O((V+E)(c+1))`, where `c`
is the number of cycles.

Risk propagation uses a centralized default of four hops. Approved deep
forensic sweeps can use the explicit `ScoreWithHops` or `TraverseWithHops`
override. Velocity is calculated as USD volume per configured window (15
minutes is a typical setting) and applies an exponential multiplier only above
the configured threshold.

Redis adjacency caching uses JSON neighbor lists with shorter TTLs for
high-degree wallets. The deployment Redis configuration enables
`maxmemory 2gb` and `maxmemory-policy allkeys-lru`.
