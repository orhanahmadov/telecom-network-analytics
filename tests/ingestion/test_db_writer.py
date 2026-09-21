import re
from pathlib import Path

from ingestion.producer.db_writer import INSERT_SQL, SOURCE_COLUMNS, to_row
from ingestion.producer.generator import generate_event

ROOT = Path(__file__).resolve().parents[2]


def _table_columns(sql_file: Path, table: str) -> list[str]:
    """Column names of `CREATE TABLE ... <table> (...)` in an init script."""
    text = re.sub(r"--[^\n]*", "", sql_file.read_text())
    body = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)}\s*\((.*?)\n\);", text, re.S).group(1)
    return [line.split()[0] for line in body.strip().splitlines() if line.strip()]


def test_insert_columns_exist_in_source_table():
    ddl_columns = _table_columns(ROOT / "infra/postgres-source/init.sql", "source.network_events")
    assert set(SOURCE_COLUMNS) <= set(ddl_columns)
    # the only DDL column the writer does not set is the server-side default
    assert set(ddl_columns) - set(SOURCE_COLUMNS) == {"inserted_at"}


def test_raw_table_mirrors_source_table():
    src = set(_table_columns(ROOT / "infra/postgres-source/init.sql", "source.network_events")) - {"inserted_at"}
    raw = set(_table_columns(ROOT / "infra/postgres/init.sql", "raw.network_events"))
    assert src <= raw
    assert raw - src == {"_source_object_key", "_cdc_source_ts_ms", "_loaded_at"}


def test_to_row_produces_every_insert_parameter():
    row = to_row(generate_event())
    assert set(row) == set(SOURCE_COLUMNS)
    for column in SOURCE_COLUMNS:
        assert f"%({column})s" in INSERT_SQL


def test_drift_fields_land_in_extra_column():
    row = to_row(generate_event(dirty_record_rate=0.0, schema_drift_rate=1.0))
    assert "volte_enabled" in row["extra"]
    assert "volte_enabled" not in row  # never becomes a top-level column
