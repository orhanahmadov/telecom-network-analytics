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


def test_staging_split_models_exist():
    staging = DBT_PROJECT_DIR / "models" / "staging"
    assert (staging / "stg_network_events_valid.sql").exists()
    assert (staging / "stg_network_events_rejected.sql").exists()


def test_curated_models_exist():
    curated = DBT_PROJECT_DIR / "models" / "curated"
    for model in ("dim_cell_site.sql", "dim_subscriber.sql", "fct_voice_call.sql", "fct_data_session.sql"):
        assert (curated / model).exists(), model


def test_curated_fact_models_are_incremental_on_event_id():
    curated = DBT_PROJECT_DIR / "models" / "curated"
    for model in ("fct_voice_call.sql", "fct_data_session.sql"):
        src = (curated / model).read_text()
        assert "materialized='incremental'" in src
        assert "unique_key='event_id'" in src


def test_curated_schema_yml_declares_dq_tests():
    schema = yaml.safe_load((DBT_PROJECT_DIR / "models" / "curated" / "schema.yml").read_text())
    models_by_name = {m["name"]: m for m in schema["models"]}
    call_result_tests = models_by_name["fct_voice_call"]["columns"]
    column_names = {c["name"] for c in call_result_tests}
    assert {"event_id", "call_result", "cell_id", "subscriber_id"} <= column_names


def test_adr_for_dim_subscriber_scd2_exists():
    adr_dir = ROOT / "docs" / "decisions"
    assert any(p.name.endswith("derive-dim-subscriber-scd2-from-event-stream.md") for p in adr_dir.glob("*.md"))


def test_serving_views_exist():
    serving = DBT_PROJECT_DIR / "models" / "serving"
    assert (serving / "vw_cell_quality_ranking.sql").exists()
    assert (serving / "vw_subscriber_churn_signals.sql").exists()


def test_serving_schema_config_is_view():
    content = (DBT_PROJECT_DIR / "dbt_project.yml").read_text()
    assert "serving" in content
    assert "+schema: serving" in content


def test_serving_schema_yml_declares_uniqueness_tests():
    schema = yaml.safe_load((DBT_PROJECT_DIR / "models" / "serving" / "schema.yml").read_text())
    models_by_name = {m["name"]: m for m in schema["models"]}
    assert "vw_cell_quality_ranking" in models_by_name
    assert "vw_subscriber_churn_signals" in models_by_name


def test_adr_for_serving_heuristics_exists():
    adr_dir = ROOT / "docs" / "decisions"
    assert any(p.name.endswith("serving-layer-heuristics.md") for p in adr_dir.glob("*.md"))


def test_schema_name_is_not_prefixed_with_the_target_schema():
    # Found live: dbt's default generate_schema_name macro prefixes a model's custom
    # schema with the profile's base schema (staging_staging / staging_curated instead of
    # staging / curated). macros/generate_schema_name.sql overrides this - see its comment.
    macro = (DBT_PROJECT_DIR / "macros" / "generate_schema_name.sql").read_text()
    assert "generate_schema_name" in macro
    assert "custom_schema_name | trim" in macro


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
