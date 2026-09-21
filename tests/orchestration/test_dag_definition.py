import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parents[2] / "orchestration" / "dags" / "telecom_pipeline_dag.py"


def _source() -> str:
    return DAG_FILE.read_text()


def test_dag_file_is_valid_python():
    ast.parse(_source())  # Airflow is not installed on the host; syntax check only


def test_dag_ids_and_tasks_are_declared():
    src = _source()
    assert 'dag_id="telecom_pipeline"' in src
    for task_id in ("load_raw_to_postgres", "dbt_run_staging", "dbt_run_curated", "spark_batch_cell_hourly_kpi"):
        assert f'task_id="{task_id}"' in src


def test_dependency_graph_matches_the_plan():
    src = _source()
    assert "load_raw >> dbt_run_staging >> dbt_run_curated" in src
    assert "load_raw >> spark_cell_hourly_kpi" in src


def test_dbt_binary_matches_the_airflow_dockerfile():
    dockerfile = (DAG_FILE.parents[2] / "infra" / "airflow" / "Dockerfile").read_text()
    assert "/home/airflow/dbt-venv" in dockerfile
    assert "/home/airflow/dbt-venv/bin/dbt" in _source()
