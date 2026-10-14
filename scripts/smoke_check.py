"""
Post-startup smoke check: are the moving parts of the pipeline actually wired together,
and is data actually flowing end to end through every layer?

`docker compose ps` proves every container is healthy. It cannot prove that the Debezium
connector is registered and RUNNING, that the warehouse schemas exist, that the object
store answers with the credentials in .env, or that a row generated at the source has
actually made it all the way through raw -> staging -> curated -> serving. This script
checks exactly those things, ending with a row count per layer (docs/PROJECT_PLAN.md,
section 5) so a glance at the output answers "did the whole pipeline run end to end?".

  * read-only: it never creates, changes or deletes anything;
  * independent: one failing check never hides the others;
  * exit code 1 only on FAIL (a WARN, e.g. "topic not created yet", does not fail the run).

Usage (stack must be up, and the DAG should have run at least once for layer counts to be
non-zero):
    make smoke
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Callable

from config.settings import settings

CONNECTOR_NAME = "telecom-postgres-source-connector"

OK, WARN, FAIL = "OK", "WARN", "FAIL"
Result = tuple[str, str]  # (status, detail)


def evaluate_connector_status(status: dict) -> Result:
    """Pure function: interpret the JSON from GET /connectors/<name>/status."""
    tasks = status.get("tasks", [])
    if not tasks:
        return FAIL, "connector has no tasks"
    states = [status.get("connector", {}).get("state")] + [t.get("state") for t in tasks]
    not_running = [s for s in states if s != "RUNNING"]
    if not_running:
        return FAIL, f"states not RUNNING: {not_running}"
    return OK, f"connector and {len(tasks)} task(s) RUNNING"


def check_connector() -> Result:
    url = f"{settings.kafka_connect.url.rstrip('/')}/connectors/{CONNECTOR_NAME}/status"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return evaluate_connector_status(json.load(response))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return FAIL, "connector not registered - run `make register-connector`"
        raise


def check_topic() -> Result:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(bootstrap_servers=settings.kafka.bootstrap_servers, consumer_timeout_ms=5000)
    try:
        topics = consumer.topics()
    finally:
        consumer.close()
    if settings.kafka.topic_network_events in topics:
        return OK, f"topic {settings.kafka.topic_network_events} exists"
    return WARN, "topic not created yet (Debezium creates it with the first event - run `make generate`)"


def check_object_store() -> Result:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=settings.rustfs.endpoint_url,
        aws_access_key_id=settings.rustfs.access_key,
        aws_secret_access_key=settings.rustfs.secret_key,
    )
    buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    return OK, f"RustFS answers with the configured credentials; buckets: {buckets or 'none yet'}"


def _warehouse_cursor():
    """Open a short-lived connection to the warehouse Postgres. Caller uses it as a context manager."""
    import psycopg2

    cfg = settings.postgres
    return psycopg2.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, dbname=cfg.db, connect_timeout=5
    )


def check_warehouse() -> Result:
    with _warehouse_cursor() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select count(*) from information_schema.schemata "
                "where schema_name in ('raw', 'staging', 'curated', 'serving')"
            )
            schemas = cur.fetchone()[0]
            cur.execute("select to_regclass('raw.network_events') is not null")
            table = cur.fetchone()[0]
    if schemas != 4 or not table:
        return FAIL, f"expected 4 schemas + raw.network_events, found {schemas} schema(s), table={table}"
    return OK, "schemas raw/staging/curated/serving and raw.network_events exist"


def check_source_db() -> Result:
    import psycopg2

    cfg = settings.source_postgres
    with psycopg2.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, dbname=cfg.db, connect_timeout=5
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from source.network_events")
            return OK, f"source.network_events: {cur.fetchone()[0]} row(s)"


def _table_count(cur, qualified_name: str) -> int:
    # qualified_name always comes from the fixed, code-owned dicts below - never user input.
    cur.execute(f"select count(*) from {qualified_name}")
    return cur.fetchone()[0]


def _layer_row_counts(qualified_names: dict[str, str]) -> Result:
    """Count rows in each `qualified_names` (short name -> schema.table) and report as one line."""
    with _warehouse_cursor() as conn:
        with conn.cursor() as cur:
            counts = {name: _table_count(cur, qualified) for name, qualified in qualified_names.items()}
    return OK, ", ".join(f"{name}: {count} row(s)" for name, count in counts.items())


def check_raw_layer() -> Result:
    return _layer_row_counts({"raw.network_events": "raw.network_events"})


def check_staging_layer() -> Result:
    return _layer_row_counts(
        {
            "stg_network_events_valid": "staging.stg_network_events_valid",
            "stg_network_events_rejected": "staging.stg_network_events_rejected",
        }
    )


def check_curated_layer() -> Result:
    return _layer_row_counts(
        {
            "dim_cell_site": "curated.dim_cell_site",
            "dim_subscriber": "curated.dim_subscriber",
            "fct_voice_call": "curated.fct_voice_call",
            "fct_data_session": "curated.fct_data_session",
        }
    )


def check_serving_layer() -> Result:
    return _layer_row_counts(
        {
            "vw_cell_quality_ranking": "serving.vw_cell_quality_ranking",
            "vw_subscriber_churn_signals": "serving.vw_subscriber_churn_signals",
        }
    )


# Ordered to tell the data's own story: infra wiring first, then one row-count line per
# layer, source through serving, so the printed output reads as the pipeline's shape.
CHECKS: dict[str, Callable[[], Result]] = {
    "debezium connector": check_connector,
    "kafka topic": check_topic,
    "object store (RustFS)": check_object_store,
    "warehouse schemas": check_warehouse,
    "source layer": check_source_db,
    "raw layer": check_raw_layer,
    "staging layer": check_staging_layer,
    "curated layer": check_curated_layer,
    "serving layer": check_serving_layer,
}


def run_checks(checks: dict[str, Callable[[], Result]]) -> tuple[list[tuple[str, str, str]], int]:
    """Run every check; an exception becomes a FAIL for that check only. Returns (rows, exit_code)."""
    rows = []
    for name, check in checks.items():
        try:
            status, detail = check()
        except Exception as exc:  # noqa: BLE001 - any error means "not reachable / not wired"
            status, detail = (
                FAIL,
                f"{type(exc).__name__}: {' '.join(str(exc).split())}",
            )  # one line, even for multi-line driver errors
        rows.append((name, status, detail))
    return rows, 1 if any(status == FAIL for _, status, _ in rows) else 0


def main() -> int:
    rows, code = run_checks(CHECKS)
    for name, status, detail in rows:
        print(f"[{status:>4}] {name}: {detail}")
    return code


if __name__ == "__main__":
    sys.exit(main())
