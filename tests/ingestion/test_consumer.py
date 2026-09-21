import json

import pytest

from ingestion.consumer import kafka_to_rustfs as consumer


def test_object_key_uses_message_time_not_wall_clock():
    # 2026-03-05T10:00:00Z
    key = consumer.build_object_key(timestamp_ms=1772704800000, partition=0, first_offset=10, last_offset=209)
    assert key == "raw/network_events/dt=2026-03-05/hr=10/p0_10-209.jsonl"


def test_hour_partition_rolls_over_at_the_hour_and_day_boundary():
    before = consumer.build_object_key(timestamp_ms=1772751599000, partition=0, first_offset=1, last_offset=2)  # 2026-03-05T22:59:59Z
    after = consumer.build_object_key(timestamp_ms=1772751600000, partition=0, first_offset=1, last_offset=2)   # 23:00:00Z
    next_day = consumer.build_object_key(timestamp_ms=1772755200000, partition=0, first_offset=1, last_offset=2)  # 2026-03-06T00:00:00Z
    assert "dt=2026-03-05/hr=22/" in before
    assert "dt=2026-03-05/hr=23/" in after
    assert "dt=2026-03-06/hr=00/" in next_day


def test_same_offsets_always_produce_the_same_key():
    args = dict(timestamp_ms=1772704800000, partition=0, first_offset=5, last_offset=9)
    assert consumer.build_object_key(**args) == consumer.build_object_key(**args)


def test_ndjson_roundtrip():
    records = [{"a": 1}, {"b": [1, 2]}]
    lines = consumer.to_ndjson(records).decode().split("\n")
    assert [json.loads(line) for line in lines] == records


def test_tombstone_is_skipped_by_deserializer():
    assert consumer._deserialize(None) is None
    assert consumer._deserialize(b'{"op": "c"}') == {"op": "c"}


def test_cli_help_works_without_infrastructure():
    with pytest.raises(SystemExit) as exit_info:
        consumer.main(["--help"])
    assert exit_info.value.code == 0
