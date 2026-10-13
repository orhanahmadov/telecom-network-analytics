"""
Phase 0 skeleton DAG for the Telecom Network Analytics pipeline.

Defines the SHAPE of the batch pipeline so the orchestration layer is provable
from day one:

    load_raw_to_postgres --> dbt_run_staging --> dbt_test_staging --> dbt_run_curated
                          --> dbt_test_curated --> dbt_run_serving --> dbt_test_serving
                         \\-> spark_batch_cell_hourly_kpi

The generator, Debezium and the Kafka consumer are NOT orchestrated here: events flow
generator -> source Postgres -> Debezium -> Kafka -> consumer -> RustFS continuously
and independently. This DAG owns only the batch steps from RustFS onward.

`load_raw_to_postgres` is parameterized by the DAG run's logical date: it loads every
RustFS object under raw/network_events/dt=<logical_date>/hr=*/ (see
ingestion/loader/raw_loader.py for the idempotency guarantees). Re-running the same
logical date, whether retried by Airflow or re-triggered manually, is safe: the loader
skips objects already recorded in raw._loaded_objects and inserts rows with
ON CONFLICT (event_id) DO NOTHING (docs/PROJECT_PLAN.md, section 6, "Idempotency and
re-runs").

Every task retries twice with a delay (`default_args`) before Airflow marks the run
failed, per docs/PROJECT_PLAN.md section 6, "Failure handling". To prove that behavior
(and that a failed run leaves no partial state) without waiting for a real fault, trigger
the DAG with `{"force_failure": "load_raw_to_postgres"}` - see `load_raw_to_postgres`
below.

`dbt_test_staging` and `dbt_test_curated` are blocking gates (docs/PROJECT_PLAN.md,
section 7, checks 1/6/7): a `dbt test` failure fails that task, which Airflow leaves
every downstream dbt task in `upstream_failed` - bad data never silently reaches
`curated` or `serving`. The Spark step is still a placeholder - no business logic ships
for it yet. The DAG is created paused (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION) so it
never runs unattended before all of its logic exists. See docs/PROJECT_PLAN.md, section 8.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/project/transformation/dbt_telecom"

default_args = {
    "owner": "telecom-network-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def load_raw_to_postgres(logical_date, params, **_context) -> None:
    """Load every RustFS object for this DAG run's logical date into raw.network_events.

    `params["force_failure"] == "load_raw_to_postgres"` deliberately fails this task
    before it touches any data, to demonstrate retries-with-delay and alerting on a
    real Airflow run without corrupting state - trigger the DAG with
    `{"force_failure": "load_raw_to_postgres"}` to exercise it, then re-trigger with the
    default `"none"` to see the same logical date load normally and idempotently.
    """
    date = logical_date.strftime("%Y-%m-%d")

    if params.get("force_failure") == "load_raw_to_postgres":
        logger.error(
            "event=deliberate_failure task=load_raw_to_postgres date=%s "
            "reason=force_failure_param - no data was read or written",
            date,
        )
        raise RuntimeError(
            "Deliberate failure requested via the 'force_failure' DAG param "
            "(docs/PROJECT_PLAN.md section 6, Failure handling). No data was touched; "
            "re-trigger with force_failure=none to run normally."
        )

    from ingestion.loader.raw_loader import run as load_raw_objects

    result = load_raw_objects(date)
    logger.info(
        "event=load_raw_to_postgres_complete date=%s objects_seen=%d objects_loaded=%d "
        "objects_skipped=%d rows_inserted=%d",
        date,
        result.objects_seen,
        result.objects_loaded,
        result.objects_skipped,
        result.rows_inserted,
    )


def spark_batch_cell_hourly_kpi(logical_date, **_context) -> None:
    """Phase 3: run transformation/spark_jobs/cell_hourly_kpi_batch.py for the logical date."""
    date = logical_date.strftime("%Y-%m-%d")
    logger.info("event=spark_batch_cell_hourly_kpi_not_implemented date=%s", date)
    raise NotImplementedError("Implemented in Phase 3 - Batch Processing & Orchestration.")


with DAG(
    dag_id="telecom_pipeline",
    description="RustFS raw -> Postgres raw -> dbt staging/curated/serving (gated by dbt test) + Spark cell KPI aggregation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={
        "force_failure": Param(
            "none",
            type="string",
            enum=["none", "load_raw_to_postgres"],
            description=(
                "Set to 'load_raw_to_postgres' to deliberately fail that task before it "
                "touches any data - proves retries-with-delay and alerting without "
                "corrupting state (docs/PROJECT_PLAN.md section 6). Leave as 'none' for "
                "a normal run."
            ),
        )
    },
    tags=["telecom", "phase-0-skeleton"],
) as dag:
    load_raw = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_to_postgres,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} run --select staging",
    )

    # Blocking DQ gate (docs/PROJECT_PLAN.md section 7, checks 1): event_id not_null/unique
    # on stg_network_events. A failure here fails this task, so dbt_run_curated (and
    # everything after it) is never triggered - Airflow marks it upstream_failed.
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} test --select staging",
    )

    dbt_run_curated = BashOperator(
        task_id="dbt_run_curated",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} run --select curated",
    )

    # Blocking DQ gate (docs/PROJECT_PLAN.md section 7, checks 6/7): accepted_values on
    # call_result, relationships from both facts to their dimensions. A failure here blocks
    # dbt_run_serving - the serving views never read a curated layer with orphaned facts.
    dbt_test_curated = BashOperator(
        task_id="dbt_test_curated",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} test --select curated",
    )

    dbt_run_serving = BashOperator(
        task_id="dbt_run_serving",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} run --select serving",
    )

    # Not a blocking gate (nothing downstream depends on serving) - still runs so a
    # regression in the two views fails the DAG run and is reported, instead of silently
    # publishing a broken business-question answer.
    dbt_test_serving = BashOperator(
        task_id="dbt_test_serving",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} test --select serving",
    )

    spark_cell_hourly_kpi = PythonOperator(
        task_id="spark_batch_cell_hourly_kpi",
        python_callable=spark_batch_cell_hourly_kpi,
    )

    (
        load_raw
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_curated
        >> dbt_test_curated
        >> dbt_run_serving
        >> dbt_test_serving
    )
    load_raw >> spark_cell_hourly_kpi
