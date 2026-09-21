"""
Phase 0 skeleton for the Spark batch job that will compute `curated.agg_cell_hourly_kpi`.

Ownership split with dbt (see docs/PROJECT_PLAN.md, sections 3 and 5):
  - dbt owns the dimensional model (dim_subscriber, dim_cell_site, fct_voice_call,
    fct_data_session) via SQL against Postgres.
  - Spark owns this one aggregate: it reads the raw NDJSON files directly from RustFS
    (S3A, one day-partition at a time) and writes cell-level hourly network KPIs
    (call attempts, drop rate, throughput, p95 latency, volume) into Postgres
    `curated` via JDBC. Cell x hour aggregation over every event is the one job that
    outgrows row-by-row SQL first as event volume rises.

Phase 0 only proves the environment (Spark master/worker + this script are reachable
via spark-submit); no aggregation logic exists yet.

Usage:
    spark-submit --master ${SPARK_MASTER_URL} \
        transformation/spark_jobs/cell_hourly_kpi_batch.py --date 2026-01-31
"""
from __future__ import annotations

import argparse
import sys


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch-compute curated.agg_cell_hourly_kpi from RustFS raw events.",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Day partition to process, YYYY-MM-DD (matches raw/network_events/dt=<date>/ in RustFS).",
    )
    return parser


def run(date: str) -> None:
    """
    Phase 3 will implement: create a SparkSession with the S3A filesystem pointed at
    RustFS, read s3a://<raw_bucket>/raw/network_events/dt=<date>/*.jsonl, parse the
    Debezium envelope (`after` payload), aggregate per cell_id x hour, and overwrite
    that day's partition in curated.agg_cell_hourly_kpi via the Postgres JDBC driver.
    """
    raise NotImplementedError("Implemented in Phase 3 - Batch Processing & Orchestration.")


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    run(date=args.date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
