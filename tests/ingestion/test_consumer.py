import json

import pytest

from ingestion.consumer import kafka_to_rustfs as consumer


def test_object_key_uses_message_time_not_wall_clock():
    # 2026-03-05T10:00:00Z
    key = consumer.build_object_key(timestamp_ms=1772704800000, partition=0, first_offset=10, last_offset=209)
    assert key == "raw/network_events/dt=2026-03-05/p0_10-209.jsonl"


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
