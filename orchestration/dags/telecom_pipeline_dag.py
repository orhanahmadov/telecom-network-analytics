"""
Phase 0 skeleton DAG for the Telecom Network Analytics pipeline.

Defines the SHAPE of the batch pipeline so the orchestration layer is provable
from day one:

    load_raw_to_postgres --> dbt_run_staging --> dbt_run_curated
                         \\-> spark_batch_cell_hourly_kpi

The generator, Debezium and the Kafka consumer are NOT orchestrated here: events flow
generator -> source Postgres -> Debezium -> Kafka -> consumer -> RustFS continuously
and independently. This DAG owns only the batch steps from RustFS onward.

Task bodies for load/spark are placeholders on purpose - no business logic ships in
Phase 0. The DAG is created paused (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION) so it
never runs unattended before its logic exists. See docs/PROJECT_PLAN.md, section 8.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/project/transformation/dbt_telecom"

default_args = {
    "owner": "telecom-network-analytics",
    "retries": 2,
}


def load_raw_to_postgres(**_context) -> None:
    """Phase 1: list new objects under raw/network_events/ in RustFS and load them into raw.network_events."""
    raise NotImplementedError("Implemented in Phase 1 - Ingestion & Raw Storage.")


def spark_batch_cell_hourly_kpi(**_context) -> None:
    """Phase 3: run transformation/spark_jobs/cell_hourly_kpi_batch.py for the logical date."""
    raise NotImplementedError("Implemented in Phase 3 - Batch Processing & Orchestration.")


with DAG(
    dag_id="telecom_pipeline",
    description="RustFS raw -> Postgres raw -> dbt staging/curated + Spark cell KPI aggregation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
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

    dbt_run_curated = BashOperator(
        task_id="dbt_run_curated",
        bash_command=f"cd {DBT_DIR} && DBT_PROFILES_DIR=. {DBT} run --select curated",
    )

    spark_cell_hourly_kpi = PythonOperator(
        task_id="spark_batch_cell_hourly_kpi",
        python_callable=spark_batch_cell_hourly_kpi,
    )

    load_raw >> dbt_run_staging >> dbt_run_curated
    load_raw >> spark_cell_hourly_kpi
