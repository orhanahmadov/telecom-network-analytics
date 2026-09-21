from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = ROOT / "transformation" / "dbt_telecom"


def test_dbt_project_yml_is_valid():
    content = yaml.safe_load((DBT_PROJECT_DIR / "dbt_project.yml").read_text())
    assert content["name"] == "dbt_telecom"
    assert content["profile"] == "dbt_telecom"


def test_profile_name_matches_profiles_yml():
    profiles = yaml.safe_load((DBT_PROJECT_DIR / "profiles.yml").read_text())
    assert "dbt_telecom" in profiles


def test_profiles_yml_contains_no_secret_literals():
    text = (DBT_PROJECT_DIR / "profiles.yml").read_text()
    assert "env_var('POSTGRES_PASSWORD')" in text


def test_staging_and_curated_dirs_exist():
    assert (DBT_PROJECT_DIR / "models" / "staging" / "stg_network_events.sql").exists()
    assert (DBT_PROJECT_DIR / "models" / "curated" / "README.md").exists()


def test_source_table_is_declared_in_raw_ddl():
    sources = yaml.safe_load((DBT_PROJECT_DIR / "models" / "staging" / "_sources.yml").read_text())
    table = sources["sources"][0]["tables"][0]["name"]
    ddl = (ROOT / "infra" / "postgres" / "init.sql").read_text()
    assert f"raw.{table}" in ddl


def test_spark_job_cli_and_skeleton_contract():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "cell_hourly_kpi_batch", ROOT / "transformation" / "spark_jobs" / "cell_hourly_kpi_batch.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # must import without pyspark installed
    with pytest.raises(SystemExit) as exit_info:
        module.main(["--help"])
    assert exit_info.value.code == 0
    with pytest.raises(NotImplementedError):
        module.run("2026-01-31")
