"""
Phase 1: loads the batch objects landed in RustFS (raw/network_events/dt=<date>/hr=*/*.jsonl)
into the warehouse table raw.network_events.

Scope: one logical date at a time (every hour partition under it), driven by the Airflow
DAG's logical date (see orchestration/dags/telecom_pipeline_dag.py).

Idempotency (two layers - see docs/PROJECT_PLAN.md, section 5 and section 8):
  * object level - raw._loaded_objects tracks which object keys have already been loaded.
    An already-loaded key is skipped entirely, so re-running the same date twice (a manual
    retry, or Airflow retrying a failed task) never re-reads objects that already made it in.
  * row level   - INSERT ... ON CONFLICT (event_id) DO NOTHING, in case the same event_id
    ever lands in two objects (e.g. right at an hour boundary).
  Each object's rows and its own row in raw._loaded_objects commit in ONE transaction, so a
  crash partway through an object rolls that object back entirely instead of marking a
  half-loaded object as done. The next run reprocesses it from scratch - safe, because of
  the row-level idempotency above.

Usage:
    python -m ingestion.loader.raw_loader --date 2026-01-31
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, timedelta

from config.settings import settings

_EPOCH = date(1970, 1, 1)

# Debezium's logical date type (io.debezium.time.Date) is epoch-days, not an ISO string.
# With schemas disabled (value.converter.schemas.enable=false, see
# infra/kafka-connect/postgres-source-connector.json) the JSON converter writes that raw
# integer straight through - there's no schema for it to format against. Confirmed live: a
# `DATE` source column lands in the flat CDC record as e.g. 19936, not "2024-07-01".
_EPOCH_DAY_FIELDS = ("subscriber_activation_date",)

OBJECT_PREFIX = "raw/network_events"

# Columns copied straight from the flat CDC record. The record Debezium emits (after the
# ExtractNewRecordState unwrap) mirrors source.network_events exactly - see
# ingestion/producer/db_writer.py's SOURCE_COLUMNS, which is the other half of this contract.
RECORD_COLUMNS = (
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
)

# Added by the Debezium connector's transforms.unwrap.add.fields=source.ts_ms (see
# infra/kafka-connect/postgres-source-connector.json); not a source.network_events column.
SOURCE_TS_MS_FIELD = "__source_ts_ms"

INSERT_SQL = (
    "INSERT INTO raw.network_events ("
    + ", ".join(RECORD_COLUMNS)
    + ", extra, _source_object_key, _cdc_source_ts_ms"
    + ") VALUES ("
    + ", ".join(f"%({c})s" for c in RECORD_COLUMNS)
    + ", %(extra)s, %(_source_object_key)s, %(_cdc_source_ts_ms)s"
    + ") ON CONFLICT (event_id) DO NOTHING"
)

IS_OBJECT_LOADED_SQL = "SELECT 1 FROM raw._loaded_objects WHERE object_key = %s"
MARK_OBJECT_LOADED_SQL = "INSERT INTO raw._loaded_objects (object_key) VALUES (%s) ON CONFLICT (object_key) DO NOTHING"


@dataclass
class LoadResult:
    date: str
    objects_seen: int
    objects_loaded: int
    objects_skipped: int
    rows_inserted: int


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load one day's RustFS raw objects into raw.network_events.")
    parser.add_argument("--date", required=True, help="Logical date to load, YYYY-MM-DD.")
    return parser


def list_date_objects(s3_client, bucket: str, date: str) -> list[str]:
    """Every object under raw/network_events/dt=<date>/ (all hour partitions), sorted.

    Sorted so a run always loads a day's objects in the same order - irrelevant for
    correctness (each object commits independently) but makes logs and retries easy to follow.
    """
    prefix = f"{OBJECT_PREFIX}/dt={date}/"
    keys: list[str] = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in response.get("Contents", []))
        if not response.get("IsTruncated"):
            break
        continuation_token = response["NextContinuationToken"]
    return sorted(keys)


def _decode_epoch_day(value):
    """Debezium's epoch-days integer -> an ISO date string Postgres's DATE column accepts."""
    if isinstance(value, bool) or not isinstance(value, int):
        return value  # already a string (or None) - nothing to decode
    return (_EPOCH + timedelta(days=value)).isoformat()


def record_to_row(record: dict, object_key: str) -> dict:
    """Map one flat CDC record (as landed in RustFS) to a raw.network_events row dict."""
    known = set(RECORD_COLUMNS)
    extra = {k: v for k, v in record.items() if k not in known and k != SOURCE_TS_MS_FIELD}
    row = {column: record.get(column) for column in RECORD_COLUMNS}
    for field in _EPOCH_DAY_FIELDS:
        row[field] = _decode_epoch_day(row[field])
    row["extra"] = json.dumps(extra)
    row["_source_object_key"] = object_key
    row["_cdc_source_ts_ms"] = record.get(SOURCE_TS_MS_FIELD)
    return row


def _is_object_loaded(cur, object_key: str) -> bool:
    cur.execute(IS_OBJECT_LOADED_SQL, (object_key,))
    return cur.fetchone() is not None


def load_object(cur, object_body: bytes, object_key: str) -> int:
    """Insert every record in one object's NDJSON body and mark the object loaded.

    Runs entirely on the caller's cursor/transaction - the caller commits (or rolls back)
    this object as a single unit.
    """
    rows_inserted = 0
    for line in object_body.decode("utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        cur.execute(INSERT_SQL, record_to_row(record, object_key))
        rows_inserted += cur.rowcount
    cur.execute(MARK_OBJECT_LOADED_SQL, (object_key,))
    return rows_inserted


def run(date: str) -> LoadResult:
    # Imported lazily so this module (and its pure helpers above) can be unit-tested
    # without boto3 / psycopg2 reaching a live RustFS or Postgres.
    import boto3
    import psycopg2

    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.rustfs.endpoint_url,
        aws_access_key_id=settings.rustfs.access_key,
        aws_secret_access_key=settings.rustfs.secret_key,
    )
    keys = list_date_objects(s3_client, settings.rustfs.raw_bucket, date)

    conn = psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        user=settings.postgres.user,
        password=settings.postgres.password,
        dbname=settings.postgres.db,
    )
    conn.autocommit = False

    objects_loaded = objects_skipped = rows_inserted = 0
    try:
        for key in keys:
            with conn.cursor() as cur:
                if _is_object_loaded(cur, key):
                    objects_skipped += 1
                    conn.rollback()
                    continue
                body = s3_client.get_object(Bucket=settings.rustfs.raw_bucket, Key=key)["Body"].read()
                rows_inserted += load_object(cur, body, key)
            conn.commit()  # object's rows + its _loaded_objects row land together
            objects_loaded += 1
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return LoadResult(
        date=date,
        objects_seen=len(keys),
        objects_loaded=objects_loaded,
        objects_skipped=objects_skipped,
        rows_inserted=rows_inserted,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run(date=args.date)
    print(
        f"[raw_loader] date={result.date} objects_seen={result.objects_seen} "
        f"objects_loaded={result.objects_loaded} objects_skipped={result.objects_skipped} "
        f"rows_inserted={result.rows_inserted}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
