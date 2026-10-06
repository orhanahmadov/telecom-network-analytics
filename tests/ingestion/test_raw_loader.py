import json

from ingestion.loader import raw_loader


def test_record_to_row_maps_known_columns_and_debezium_timestamp():
    record = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "event_type": "voice_call",
        "event_time_raw": "2026-01-31T10:00:00Z",
        "subscriber_id": "22222222-2222-2222-2222-222222222222",
        "subscriber_msisdn_masked": "+994XX1234",
        "subscriber_plan": "postpaid",
        "subscriber_activation_date": "2024-01-01",
        "cell_id": "cell-01",
        "cell_region": "baku",
        "cell_radio_tech": "LTE",
        "signal_dbm": -85.0,
        "duration_sec": 120,
        "call_result": "completed",
        "data_volume_mb": None,
        "avg_throughput_mbps": None,
        "latency_ms": 45.0,
        "__source_ts_ms": 1772704800000,
    }

    row = raw_loader.record_to_row(record, object_key="raw/network_events/dt=2026-01-31/hr=10/p0_0-0.jsonl")

    for column in raw_loader.RECORD_COLUMNS:
        assert row[column] == record[column]
    assert row["_cdc_source_ts_ms"] == 1772704800000
    assert row["_source_object_key"] == "raw/network_events/dt=2026-01-31/hr=10/p0_0-0.jsonl"
    assert json.loads(row["extra"]) == {}


def test_record_to_row_routes_undeclared_fields_to_extra_and_drops_the_source_timestamp():
    record = {column: None for column in raw_loader.RECORD_COLUMNS}
    record["__source_ts_ms"] = 123
    record["promo_code"] = "SPRING26"  # schema drift: not a known column

    row = raw_loader.record_to_row(record, object_key="k")

    extra = json.loads(row["extra"])
    assert extra == {"promo_code": "SPRING26"}
    assert "__source_ts_ms" not in extra


class _FakeS3Client:
    """Minimal stand-in for boto3's S3 client, paginated like list_objects_v2."""

    def __init__(self, pages: list[list[str]]):
        self._pages = pages

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803 - mirrors boto3's signature
        page_index = 0 if ContinuationToken is None else int(ContinuationToken)
        keys = self._pages[page_index]
        is_last_page = page_index == len(self._pages) - 1
        response = {"Contents": [{"Key": k} for k in keys if k.startswith(Prefix)]}
        if not is_last_page:
            response["IsTruncated"] = True
            response["NextContinuationToken"] = str(page_index + 1)
        return response


def test_list_date_objects_filters_by_date_prefix():
    client = _FakeS3Client(
        [
            [
                "raw/network_events/dt=2026-01-31/hr=10/p0_0-99.jsonl",
                "raw/network_events/dt=2026-01-31/hr=09/p0_0-50.jsonl",
                "raw/network_events/dt=2026-02-01/hr=00/p0_0-10.jsonl",  # different day, must be excluded
            ]
        ]
    )

    keys = raw_loader.list_date_objects(client, bucket="raw-bucket", date="2026-01-31")

    assert keys == [
        "raw/network_events/dt=2026-01-31/hr=09/p0_0-50.jsonl",
        "raw/network_events/dt=2026-01-31/hr=10/p0_0-99.jsonl",
    ]


def test_list_date_objects_follows_pagination():
    client = _FakeS3Client(
        [
            ["raw/network_events/dt=2026-01-31/hr=00/p0_0-1.jsonl"],
            ["raw/network_events/dt=2026-01-31/hr=01/p0_0-1.jsonl"],
        ]
    )

    keys = raw_loader.list_date_objects(client, bucket="raw-bucket", date="2026-01-31")

    assert keys == [
        "raw/network_events/dt=2026-01-31/hr=00/p0_0-1.jsonl",
        "raw/network_events/dt=2026-01-31/hr=01/p0_0-1.jsonl",
    ]


class _FakeCursor:
    """In-memory stand-in for a psycopg2 cursor: enough to exercise load_object's logic."""

    def __init__(self, raw_network_events: dict, loaded_objects: set):
        self._raw_network_events = raw_network_events
        self._loaded_objects = loaded_objects
        self.rowcount = 0

    def execute(self, sql, params=None):
        if sql is raw_loader.MARK_OBJECT_LOADED_SQL:
            self._loaded_objects.add(params[0])
            self.rowcount = 1
        elif sql is raw_loader.INSERT_SQL:
            event_id = params["event_id"]
            if event_id in self._raw_network_events:
                self.rowcount = 0  # ON CONFLICT (event_id) DO NOTHING
            else:
                self._raw_network_events[event_id] = params
                self.rowcount = 1
        else:
            raise AssertionError(f"unexpected SQL in fake cursor: {sql!r}")


def test_load_object_inserts_every_line_and_marks_the_object_loaded():
    table: dict = {}
    loaded: set = set()
    cur = _FakeCursor(table, loaded)
    body = (
        json.dumps({**{c: None for c in raw_loader.RECORD_COLUMNS}, "event_id": "a"}).encode()
        + b"\n"
        + json.dumps({**{c: None for c in raw_loader.RECORD_COLUMNS}, "event_id": "b"}).encode()
    )

    rows_inserted = raw_loader.load_object(cur, body, object_key="k1")

    assert rows_inserted == 2
    assert set(table) == {"a", "b"}
    assert "k1" in loaded


def test_load_object_skips_blank_lines_and_is_row_level_idempotent_on_replay():
    table: dict = {}
    loaded: set = set()
    body = b"\n" + json.dumps({**{c: None for c in raw_loader.RECORD_COLUMNS}, "event_id": "a"}).encode() + b"\n\n"

    first_run = raw_loader.load_object(_FakeCursor(table, loaded), body, object_key="k1")
    second_run = raw_loader.load_object(_FakeCursor(table, loaded), body, object_key="k1")

    assert first_run == 1
    assert second_run == 0  # same event_id again -> ON CONFLICT DO NOTHING, nothing new inserted
    assert len(table) == 1
