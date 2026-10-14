from scripts import smoke_check as sc


def test_running_connector_with_running_tasks_is_ok():
    status = {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]}
    assert sc.evaluate_connector_status(status)[0] == sc.OK


def test_failed_task_is_a_failure_even_if_the_connector_is_running():
    status = {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "FAILED"}]}
    result, detail = sc.evaluate_connector_status(status)
    assert result == sc.FAIL and "FAILED" in detail


def test_paused_connector_is_a_failure():
    status = {"connector": {"state": "PAUSED"}, "tasks": [{"id": 0, "state": "PAUSED"}]}
    assert sc.evaluate_connector_status(status)[0] == sc.FAIL


def test_connector_without_tasks_is_a_failure():
    assert sc.evaluate_connector_status({"connector": {"state": "RUNNING"}, "tasks": []})[0] == sc.FAIL


def test_one_failing_check_does_not_hide_the_others():
    def boom():
        raise ConnectionError("refused")

    rows, code = sc.run_checks({"a": lambda: (sc.OK, "fine"), "b": boom, "c": lambda: (sc.OK, "fine")})
    assert [r[0] for r in rows] == ["a", "b", "c"]
    assert rows[1][1] == sc.FAIL and "ConnectionError" in rows[1][2]
    assert code == 1


def test_warning_does_not_fail_the_run():
    rows, code = sc.run_checks({"topic": lambda: (sc.WARN, "not yet")})
    assert code == 0 and rows[0][1] == sc.WARN


def test_all_ok_exits_zero():
    assert sc.run_checks({"a": lambda: (sc.OK, "x")})[1] == 0


def test_connector_name_matches_the_connector_config():
    import json
    from pathlib import Path

    config = json.loads(
        (Path(__file__).resolve().parents[2] / "infra/kafka-connect/postgres-source-connector.json").read_text()
    )
    assert config["name"] == sc.CONNECTOR_NAME


def test_checks_cover_every_pipeline_layer():
    assert set(sc.CHECKS) == {
        "debezium connector",
        "kafka topic",
        "object store (RustFS)",
        "warehouse schemas",
        "source layer",
        "raw layer",
        "staging layer",
        "curated layer",
        "serving layer",
    }


class _FakeCursor:
    """Returns one queued row per `execute()` call, in order - enough for a row-count query."""

    def __init__(self, rows):
        self._rows = iter(rows)
        self.queries = []

    def execute(self, query):
        self.queries.append(query)

    def fetchone(self):
        return (next(self._rows),)


def test_table_count_runs_a_count_query_and_returns_the_row():
    cur = _FakeCursor([42])
    assert sc._table_count(cur, "curated.dim_cell_site") == 42
    assert cur.queries == ["select count(*) from curated.dim_cell_site"]


def test_layer_row_counts_reports_every_table_on_one_line(monkeypatch):
    cur = _FakeCursor([481, 644])

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return _FakeCursorContext(cur)

    class _FakeCursorContext:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            return self._inner

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(sc, "_warehouse_cursor", lambda: _FakeConn())
    status, detail = sc._layer_row_counts(
        {"fct_voice_call": "curated.fct_voice_call", "fct_data_session": "curated.fct_data_session"}
    )
    assert status == sc.OK
    assert detail == "fct_voice_call: 481 row(s), fct_data_session: 644 row(s)"
