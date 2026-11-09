#!/usr/bin/env python3
"""Stream Ethereum transfer history from BigQuery into partitioned Parquet files."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from google.cloud import bigquery
import pyarrow as pa
import pyarrow.parquet as pq

NATIVE_TOKEN = "0x" + "0" * 40
SCHEMA = pa.schema(
    [
        ("tx_hash", pa.binary()),
        ("block_number", pa.int64()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("from_address", pa.string()),
        ("to_address", pa.string()),
        ("token_address", pa.string()),
        ("raw_amount", pa.string()),
        ("decimals", pa.int32()),
        ("usd_value", pa.float64()),
    ]
)


def query_for(kind: str) -> str:
    if kind == "native":
        return """
        SELECT t.hash AS tx_hash, t.block_number, b.timestamp AS timestamp,
               t.from_address, t.to_address, @native_token AS token_address,
               CAST(t.value AS STRING) AS raw_amount, 18 AS decimals,
               CAST(NULL AS FLOAT64) AS usd_value
        FROM `bigquery-public-data.crypto_ethereum.transactions` t
        JOIN `bigquery-public-data.crypto_ethereum.blocks` b USING (block_number)
        WHERE t.value > 0 AND t.to_address IS NOT NULL
          AND t.block_number BETWEEN @start_block AND @end_block
        ORDER BY t.block_number, t.transaction_index
        """
    return """
        SELECT t.transaction_hash AS tx_hash, t.block_number, b.timestamp AS timestamp,
               t.from_address, t.to_address, t.token_address,
               CAST(t.value AS STRING) AS raw_amount, 0 AS decimals,
               CAST(NULL AS FLOAT64) AS usd_value
        FROM `bigquery-public-data.crypto_ethereum.token_transfers` t
        JOIN `bigquery-public-data.crypto_ethereum.blocks` b USING (block_number)
        WHERE t.block_number BETWEEN @start_block AND @end_block
        ORDER BY t.block_number
    """


def rows(client: bigquery.Client, kind: str, start: int, end: int):
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_block", "INT64", start),
            bigquery.ScalarQueryParameter("end_block", "INT64", end),
            bigquery.ScalarQueryParameter("native_token", "STRING", NATIVE_TOKEN),
        ],
        use_query_cache=True,
    )
    # BigQuery Storage API pages results without materializing the full query.
    return client.query(query_for(kind), job_config=config).result(page_size=10_000)


def write_kind(client: bigquery.Client, kind: str, start: int, end: int, output: Path) -> int:
    writer: pq.ParquetWriter | None = None
    count = 0
    try:
        for batch in rows(client, kind, start, end).to_arrow_iterable():
            values = batch.to_pydict()
            table = pa.Table.from_arrays(
                [
                    pa.array([bytes.fromhex(value[2:] if value.startswith("0x") else value)
                              for value in values["tx_hash"]], type=pa.binary()),
                    pa.array(values["block_number"], type=pa.int64()),
                    pa.array(values["timestamp"], type=SCHEMA.field("timestamp").type),
                    pa.array(values["from_address"], type=pa.string()),
                    pa.array(values["to_address"], type=pa.string()),
                    pa.array(values["token_address"], type=pa.string()),
                    pa.array(values["raw_amount"], type=pa.string()),
                    pa.array(values["decimals"], type=pa.int32()),
                    pa.array(values["usd_value"], type=pa.float64()),
                ],
                schema=SCHEMA,
            )
            if writer is None:
                output.parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(output, SCHEMA, compression="zstd")
            writer.write_table(table)
            count += table.num_rows
    finally:
        if writer:
            writer.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("parquet"))
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    if args.start_block < 0 or args.end_block < args.start_block:
        parser.error("invalid block range")

    client = bigquery.Client(project=args.project)
    for kind in ("native", "erc20"):
        output = args.output_dir / f"{kind}_{args.start_block}_{args.end_block}.parquet"
        count = write_kind(client, kind, args.start_block, args.end_block, output)
        print(f"{kind}: wrote {count} rows to {output}")


if __name__ == "__main__":
    main()
