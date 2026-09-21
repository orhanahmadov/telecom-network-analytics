from datetime import datetime

import pytest

from ingestion.producer import generator
from ingestion.producer.generator import build_pools, generate_event


def _is_dirty(e: dict) -> bool:
    """True when the event carries at least one of the documented corruptions."""
    try:
        datetime.fromisoformat(e["event_time"])
        bad_time = False
    except ValueError:
        bad_time = True
    return (
        e["subscriber"]["subscriber_id"] is None
        or bad_time
        or (e["duration_sec"] is not None and e["duration_sec"] < 0)
        or (e["data_volume_mb"] is not None and e["data_volume_mb"] < 0)
        or (e["signal_dbm"] is not None and e["signal_dbm"] > 0)
    )


def test_event_has_required_fields():
    event = generate_event(dirty_record_rate=0.0, schema_drift_rate=0.0)
    for key in ("event_id", "event_type", "event_time", "subscriber", "cell", "signal_dbm"):
        assert key in event
    assert event["event_type"] in {"voice_call", "data_session", "sms"}


def test_clean_events_are_valid():
    for _ in range(300):
        e = generate_event(dirty_record_rate=0.0, schema_drift_rate=0.0)
        assert not _is_dirty(e)
        assert -140.0 <= e["signal_dbm"] <= -44.0


def test_dirty_rate_one_corrupts_every_event():
    # Regression guard: every dirty event must actually be corrupted (incl. SMS events,
    # which have no duration/volume to corrupt).
    events = [generate_event(dirty_record_rate=1.0, schema_drift_rate=0.0) for _ in range(300)]
    assert all(_is_dirty(e) for e in events)


def test_schema_drift_adds_undocumented_field():
    drifted = [generate_event(dirty_record_rate=0.0, schema_drift_rate=1.0) for _ in range(20)]
    assert all("volte_enabled" in e for e in drifted)


def test_hidden_ground_truth_never_leaks():
    for _ in range(100):
        assert "degraded" not in generate_event()["cell"]


def test_dirtiness_does_not_mutate_shared_subscriber_pool():
    generator.init_pools(subscriber_pool_size=50, cell_count=10, seed=1)
    subscribers, _ = generator._get_pools()
    for _ in range(300):
        generate_event(dirty_record_rate=1.0, schema_drift_rate=0.0)
    assert all(s.subscriber_id is not None for s in subscribers)


def test_pools_are_deterministic_for_a_seed():
    a_subs, a_cells = build_pools(20, 8, seed=7)
    b_subs, b_cells = build_pools(20, 8, seed=7)
    assert a_subs == b_subs and a_cells == b_cells


def test_some_cells_are_degraded():
    _, cells = build_pools(10, 200, seed=3)
    share = sum(c.degraded for c in cells) / len(cells)
    assert 0.03 < share < 0.25


def test_degraded_cells_drop_more_calls():
    def drop_rate(degraded: bool) -> float:
        cell = next(c for c in generator._get_pools()[1] if c.degraded == degraded)
        dropped = total = 0
        for _ in range(3000):
            ev = generator.NetworkEvent(
                event_id="x", event_type=generator.EventType.VOICE_CALL, event_time="t",
                subscriber=generator._get_pools()[0][0], cell=cell,
            )
            generator._fill_voice(ev)
            total += 1
            dropped += ev.call_result == "dropped"
        return dropped / total

    generator.init_pools(subscriber_pool_size=20, cell_count=60, seed=42)
    assert drop_rate(True) > 3 * drop_rate(False)
