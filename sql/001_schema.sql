CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS entities (
    address VARCHAR(42) PRIMARY KEY,
    entity_name VARCHAR(255) NOT NULL,
    category VARCHAR(32) NOT NULL CHECK (category IN (
        'EXCHANGE', 'MIXER', 'SANCTIONED_OFAC', 'DEFI_PAIR', 'HOT_WALLET', 'OTHER'
    )),
    is_high_degree BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS edges (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    tx_hash BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    raw_amount NUMERIC(78, 0) NOT NULL,
    decimals INTEGER NOT NULL DEFAULT 18 CHECK (decimals BETWEEN 0 AND 255),
    usd_value DOUBLE PRECISION,
    PRIMARY KEY (block_number, id),
    CHECK (from_address ~ '^0x[0-9a-fA-F]{40}$'),
    CHECK (to_address ~ '^0x[0-9a-fA-F]{40}$'),
    CHECK (token_address ~ '^0x[0-9a-fA-F]{40}$'),
    CHECK (raw_amount >= 0),
    CHECK (usd_value IS NULL OR usd_value >= 0)
) PARTITION BY RANGE (block_number);

-- Create additional yearly/range partitions before loading a new chain segment.
CREATE TABLE IF NOT EXISTS edges_before_2020 PARTITION OF edges
    FOR VALUES FROM (MINVALUE) TO (9193266);
CREATE TABLE IF NOT EXISTS edges_2020_2021 PARTITION OF edges
    FOR VALUES FROM (9193266) TO (13916166);
CREATE TABLE IF NOT EXISTS edges_2022_2023 PARTITION OF edges
    FOR VALUES FROM (13916166) TO (18908840);
CREATE TABLE IF NOT EXISTS edges_2024_2025 PARTITION OF edges
    FOR VALUES FROM (18908840) TO (24500000);
CREATE TABLE IF NOT EXISTS edges_2026_plus PARTITION OF edges
    FOR VALUES FROM (24500000) TO (MAXVALUE);

CREATE INDEX IF NOT EXISTS edges_from_timestamp_idx
    ON edges (from_address, timestamp DESC, block_number DESC);
CREATE INDEX IF NOT EXISTS edges_to_timestamp_idx
    ON edges (to_address, timestamp DESC, block_number DESC);
CREATE INDEX IF NOT EXISTS edges_tx_hash_idx ON edges (tx_hash);
CREATE INDEX IF NOT EXISTS edges_token_block_idx ON edges (token_address, block_number);
CREATE INDEX IF NOT EXISTS edges_timestamp_brin_idx
    ON edges USING BRIN (timestamp, block_number)
    WITH (pages_per_range = 128, autosummarize = on);

COMMENT ON TABLE edges IS
    'Append-only canonical transfer graph. Partition ranges must be extended before the chain reaches them.';
COMMENT ON COLUMN edges.raw_amount IS
    'Unsigned token base units. Keep as NUMERIC(78,0); never convert to float64.';
