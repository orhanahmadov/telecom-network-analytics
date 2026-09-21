"""
Post-startup smoke check: are the moving parts of the pipeline actually wired together?

`docker compose ps` proves every container is healthy. It cannot prove that the Debezium
connector is registered and RUNNING, that the warehouse schemas exist, or that the object
store answers with the credentials in .env. This script checks exactly those things.

  * read-only: it never creates, changes or deletes anything;
  * independent: one failing check never hides the others;
  * exit code 1 only on FAIL (a WARN, e.g. "topic not created yet", does not fail the run).

Usage (stack must be up):
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


def check_source_db() -> Result:
    import psycopg2

    cfg = settings.source_postgres
    with psycopg2.connect(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, dbname=cfg.db, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from source.network_events")
            return OK, f"source.network_events reachable, {cur.fetchone()[0]} row(s)"


def check_warehouse() -> Result:
    import psycopg2

    cfg = settings.postgres
    with psycopg2.connect(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, dbname=cfg.db, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from information_schema.schemata where schema_name in ('raw', 'staging', 'curated')")
            schemas = cur.fetchone()[0]
            cur.execute("select to_regclass('raw.network_events') is not null")
            table = cur.fetchone()[0]
    if schemas != 3 or not table:
        return FAIL, f"expected 3 schemas + raw.network_events, found {schemas} schema(s), table={table}"
    return OK, "schemas raw/staging/curated and raw.network_events exist"


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


CHECKS: dict[str, Callable[[], Result]] = {
    "debezium connector": check_connector,
    "source database": check_source_db,
    "warehouse database": check_warehouse,
    "object store (RustFS)": check_object_store,
    "kafka topic": check_topic,
}


def run_checks(checks: dict[str, Callable[[], Result]]) -> tuple[list[tuple[str, str, str]], int]:
    """Run every check; an exception becomes a FAIL for that check only. Returns (rows, exit_code)."""
    rows = []
    for name, check in checks.items():
        try:
            status, detail = check()
        except Exception as exc:  # noqa: BLE001 - any error means "not reachable / not wired"
            status, detail = FAIL, f"{type(exc).__name__}: {' '.join(str(exc).split())}"  # one line, even for multi-line driver errors
        rows.append((name, status, detail))
    return rows, 1 if any(status == FAIL for _, status, _ in rows) else 0


def main() -> int:
    rows, code = run_checks(CHECKS)
    for name, status, detail in rows:
        print(f"[{status:>4}] {name}: {detail}")
    return code


if __name__ == "__main__":
    sys.exit(main())
