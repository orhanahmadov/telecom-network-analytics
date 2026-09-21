"""
CLI entrypoint: writes synthetic network events as INSERTs into the source
Postgres OLTP table (`source.network_events`) instead of publishing to Kafka.
Kafka Connect's Debezium PostgreSQL connector (see infra/kafka-connect/) picks the
inserts up via logical replication (CDC) and streams them onto Kafka - the
generator never talks to Kafka itself.

Pipeline: this script -> Postgres (source) -> Debezium -> Kafka ->
ingestion/consumer/kafka_to_rustfs.py -> RustFS.

Usage:
    python -m ingestion.producer.db_writer --help
    python -m ingestion.producer.db_writer --count 100
    python -m ingestion.producer.db_writer --forever
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from config.settings import settings
from ingestion.producer import generator

# Single source of truth for the INSERT column list. It must match
# infra/postgres-source/init.sql (checked by tests/ingestion/test_db_writer.py).
SOURCE_COLUMNS = (
    "event_id",
    "event_type",
    "event_time_raw",
    "subscriber_id",
    "subscriber_msisdn_masked",
    "subscriber_plan",
    "subscriber_activation_date",
    "cell_id",
    "cell_region",
    "cell_radio_tech",
    "signal_dbm",
    "duration_sec",
    "call_result",
    "data_volume_mb",
    "avg_throughput_mbps",
    "latency_ms",
    "extra",
)

INSERT_SQL = (
    "INSERT INTO source.network_events ("
    + ", ".join(SOURCE_COLUMNS)
    + ") VALUES ("
    + ", ".join(f"%({c})s" for c in SOURCE_COLUMNS)
    + ") ON CONFLICT (event_id) DO NOTHING"
)

# Keys of the generator dict that are structured columns; everything else is schema drift -> `extra`.
_KNOWN_KEYS = {
    "event_id", "event_type", "event_time", "signal_dbm", "duration_sec", "call_result",
    "data_volume_mb", "avg_throughput_mbps", "latency_ms", "subscriber", "cell",
}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write synthetic network events into the source Postgres table.")
    parser.add_argument("--count", type=int, default=100, help="Number of events to write (ignored with --forever).")
    parser.add_argument("--forever", action="store_true", help="Keep writing events until interrupted (Ctrl+C).")
    parser.add_argument(
        "--rate",
        type=float,
        default=settings.generator.events_per_second,
        help="Events per second to write.",
    )
    return parser


def to_row(event: dict) -> dict:
    """Flatten a nested generator event dict into source.network_events columns."""
    subscriber, cell = event["subscriber"], event["cell"]
    extra = {k: v for k, v in event.items() if k not in _KNOWN_KEYS}
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "event_time_raw": event["event_time"],
        "subscriber_id": subscriber["subscriber_id"],
        "subscriber_msisdn_masked": subscriber["msisdn_masked"],
        "subscriber_plan": subscriber["plan"],
        "subscriber_activation_date": subscriber["activation_date"],
        "cell_id": cell["cell_id"],
        "cell_region": cell["region"],
        "cell_radio_tech": cell["radio_tech"],
        "signal_dbm": event["signal_dbm"],
        "duration_sec": event["duration_sec"],
        "call_result": event["call_result"],
        "data_volume_mb": event["data_volume_mb"],
        "avg_throughput_mbps": event["avg_throughput_mbps"],
        "latency_ms": event["latency_ms"],
        "extra": json.dumps(extra),
    }


def run(count: int, forever: bool, rate: float) -> None:
    # Imported lazily so `--help` works without psycopg2 / a reachable Postgres.
    import psycopg2

    generator.init_pools(
        subscriber_pool_size=settings.generator.subscriber_pool_size,
        cell_count=settings.generator.cell_count,
        seed=settings.generator.seed,
    )
    conn = psycopg2.connect(
        host=settings.source_postgres.host,
        port=settings.source_postgres.port,
        user=settings.source_postgres.user,
        password=settings.source_postgres.password,
        dbname=settings.source_postgres.db,
    )
    conn.autocommit = True
    delay = 1.0 / rate if rate > 0 else 0

    written = 0
    try:
        with conn.cursor() as cur:
            while forever or written < count:
                event = generator.generate_event(
                    dirty_record_rate=settings.generator.dirty_record_rate,
                    schema_drift_rate=settings.generator.schema_drift_rate,
                )
                cur.execute(INSERT_SQL, to_row(event))
                written += 1
                if delay:
                    time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()
        print(f"Wrote {written} events to source.network_events.")


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    run(count=args.count, forever=args.forever, rate=args.rate)
    return 0


if __name__ == "__main__":
    sys.exit(main())
