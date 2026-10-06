"""
CLI entrypoint: consumes the Debezium CDC topic and lands the messages, unmodified,
as newline-delimited JSON batch files in RustFS (S3-compatible object storage).

Object key: raw/network_events/dt=<YYYY-MM-DD>/hr=<HH>/p<partition>_<first_offset>-<last_offset>.jsonl
  * dt and hr (UTC) come from the Kafka message timestamp of the first record in the batch
    (NOT the wall clock), so re-processing the same offsets yields the same key even later.
  * The hour partition is the INGESTION hour: a batch belongs to the hour its first message
    arrived, so it can hold a few events from the next hour. It lets the hourly DAG address one
    slice of the lake; filtering by real event time happens later, in staging.
  * the offset range in the key makes re-runs idempotent: the same offsets overwrite the
    same object instead of duplicating data.
Offsets are committed to Kafka only AFTER the object was written (at-least-once delivery).

Phase 0 scope: a runnable skeleton. Time-based flushing of a partial batch and
multi-partition handling arrive with Phase 1 (see docs/PROJECT_PLAN.md).

Usage:
    python -m ingestion.consumer.kafka_to_rustfs --help
    python -m ingestion.consumer.kafka_to_rustfs --batch-size 500 --max-batches 1
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from config.settings import settings

OBJECT_PREFIX = "raw/network_events"
CONSUMER_GROUP = "rustfs-landing-writer"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume Kafka network events and land them as batch files in RustFS.")
    parser.add_argument("--batch-size", type=int, default=200, help="Messages to buffer before writing one object.")
    parser.add_argument(
        "--max-batches", type=int, default=None, help="Stop after writing this many batches (default: run forever)."
    )
    return parser


def build_object_key(timestamp_ms: int, partition: int, first_offset: int, last_offset: int) -> str:
    ts = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return f"{OBJECT_PREFIX}/dt={ts:%Y-%m-%d}/hr={ts:%H}/p{partition}_{first_offset}-{last_offset}.jsonl"


def to_ndjson(records: list[dict]) -> bytes:
    return "\n".join(json.dumps(r) for r in records).encode("utf-8")


def _deserialize(raw: bytes | None) -> dict | None:
    # Debezium emits a null-valued "tombstone" after a delete; there is nothing to land.
    return None if raw is None else json.loads(raw.decode("utf-8"))


def _get_s3_client():
    # Imported lazily so `--help` works without boto3 / reachable RustFS.
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.rustfs.endpoint_url,
        aws_access_key_id=settings.rustfs.access_key,
        aws_secret_access_key=settings.rustfs.secret_key,
    )


def _ensure_bucket(s3_client, bucket: str) -> None:
    existing = {b["Name"] for b in s3_client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)


def run(batch_size: int, max_batches: int | None) -> None:
    # Imported lazily so `--help` works without a reachable Kafka broker.
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        settings.kafka.topic_network_events,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_deserializer=_deserialize,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit manually, after the object is safely written
        group_id=CONSUMER_GROUP,
    )
    s3_client = _get_s3_client()
    _ensure_bucket(s3_client, settings.rustfs.raw_bucket)

    buffer: list[dict] = []
    first = None  # (offset, partition, timestamp_ms) of the first buffered message
    batches_written = 0

    try:
        for message in consumer:
            if message.value is None:
                continue
            if first is None:
                first = (message.offset, message.partition, message.timestamp)
            buffer.append(message.value)

            if len(buffer) >= batch_size:
                key = build_object_key(first[2], first[1], first[0], message.offset)
                s3_client.put_object(Bucket=settings.rustfs.raw_bucket, Key=key, Body=to_ndjson(buffer))
                consumer.commit()
                print(f"Wrote batch to s3://{settings.rustfs.raw_bucket}/{key}")
                buffer, first = [], None
                batches_written += 1
                if max_batches is not None and batches_written >= max_batches:
                    break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    run(batch_size=args.batch_size, max_batches=args.max_batches)
    return 0


if __name__ == "__main__":
    sys.exit(main())
