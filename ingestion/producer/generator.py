"""
Synthetic telecom network event generator.

Models a fixed population of subscribers roaming across a fixed set of cell sites
(so subscriber-level and cell-level analytics are possible later). About 10% of
cells are secretly "degraded": higher drop rate, lower throughput, higher latency,
weaker signal. That hidden ground truth is what the analytics must eventually
rediscover - it is never written into the emitted events.

Two deliberate impurities give downstream data-quality logic real work:

  * dirty_record_rate  - fraction of events with a broken field (null subscriber_id,
                          negative duration/volume, unparseable timestamp,
                          impossible signal strength). A corruption is ALWAYS applied
                          to a dirty event, so the configured rate is the real rate.
  * schema_drift_rate  - fraction of events carrying an undocumented attribute
                          (`volte_enabled`), simulating a probe software upgrade.

Phase 0 only requires the generator to run and emit well-formed dicts.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import asdict, replace
from datetime import datetime, timezone

from faker import Faker

from ingestion.schemas import CellSite, EventType, NetworkEvent, Subscriber

REGIONS = ["Baku", "Sumgait", "Ganja", "Lankaran", "Shaki", "Mingachevir", "Quba", "Nakhchivan"]
_PLANS = (["prepaid"] * 6) + (["postpaid"] * 3) + ["corporate"]
_TECH_WEIGHTS = {"3G": 0.2, "4G": 0.6, "5G": 0.2}
_MSISDN_PREFIXES = ["50", "51", "55", "70", "77", "99"]
DEGRADED_CELL_SHARE = 0.10

# Typical throughput (Mbps) and latency (ms) per radio technology.
_TECH_THROUGHPUT_MBPS = {"3G": 3.0, "4G": 25.0, "5G": 150.0}
_TECH_LATENCY_MS = {"3G": 90.0, "4G": 40.0, "5G": 15.0}

_SIGNAL_MIN, _SIGNAL_MAX = -140.0, -44.0  # valid RSRP-like range, dBm

_pools: tuple[list[Subscriber], list[CellSite]] | None = None


def build_pools(
    subscriber_pool_size: int = 2000,
    cell_count: int = 60,
    seed: int = 42,
) -> tuple[list[Subscriber], list[CellSite]]:
    """Deterministically build the subscriber and cell populations."""
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)

    cells = []
    for n in range(cell_count):
        region = REGIONS[n % len(REGIONS)]
        cells.append(
            CellSite(
                cell_id=f"{region[:3].upper()}-{n:04d}",
                region=region,
                radio_tech=rng.choices(list(_TECH_WEIGHTS), weights=list(_TECH_WEIGHTS.values()))[0],
                degraded=rng.random() < DEGRADED_CELL_SHARE,
            )
        )

    subscribers = []
    for _ in range(subscriber_pool_size):
        subscribers.append(
            Subscriber(
                subscriber_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                msisdn_masked=f"+994{rng.choice(_MSISDN_PREFIXES)}***{rng.randint(0, 9999):04d}",
                plan=rng.choice(_PLANS),
                activation_date=fake.date_between(start_date="-6y", end_date="-30d").isoformat(),
            )
        )
    return subscribers, cells


def init_pools(subscriber_pool_size: int = 2000, cell_count: int = 60, seed: int = 42) -> None:
    global _pools
    _pools = build_pools(subscriber_pool_size, cell_count, seed)


def _get_pools() -> tuple[list[Subscriber], list[CellSite]]:
    if _pools is None:
        init_pools()
    assert _pools is not None
    return _pools


def _signal_dbm(cell: CellSite) -> float:
    mean = -112.0 if cell.degraded else -92.0
    return round(min(max(random.gauss(mean, 8.0), _SIGNAL_MIN), _SIGNAL_MAX), 1)


def _fill_voice(event: NetworkEvent) -> None:
    drop_p, fail_p = (0.25, 0.08) if event.cell.degraded else (0.02, 0.01)
    roll = random.random()
    if roll < fail_p:
        event.call_result, event.duration_sec = "failed_setup", 0
    elif roll < fail_p + drop_p:
        event.call_result = "dropped"
        event.duration_sec = max(1, int(random.expovariate(1 / 40)))
    else:
        event.call_result = "completed"
        event.duration_sec = min(3600, max(1, int(random.expovariate(1 / 120))))


def _fill_data(event: NetworkEvent) -> None:
    tech = event.cell.radio_tech
    quality = 0.3 if event.cell.degraded else 1.0
    event.duration_sec = min(7200, max(5, int(random.expovariate(1 / 300))))
    event.data_volume_mb = round(random.lognormvariate(3.0, 1.2), 2)
    event.avg_throughput_mbps = round(_TECH_THROUGHPUT_MBPS[tech] * quality * random.lognormvariate(0, 0.4), 2)
    event.latency_ms = round(_TECH_LATENCY_MS[tech] * (2.5 if event.cell.degraded else 1.0) * random.lognormvariate(0, 0.25), 1)


def _apply_dirtiness(event: NetworkEvent) -> NetworkEvent:
    """Corrupt an otherwise-valid event. Always applies exactly one corruption."""
    options = ["null_subscriber", "bad_timestamp", "impossible_signal"]
    if event.duration_sec is not None:
        options.append("negative_duration")
    if event.data_volume_mb is not None:
        options.append("negative_volume")

    corruption = random.choice(options)
    if corruption == "null_subscriber":
        event.subscriber.subscriber_id = None
    elif corruption == "bad_timestamp":
        event.event_time = "not-a-timestamp"
    elif corruption == "impossible_signal":
        event.signal_dbm = round(random.uniform(1.0, 30.0), 1)  # positive dBm is physically impossible here
    elif corruption == "negative_duration":
        event.duration_sec = -abs(event.duration_sec) - 1
    elif corruption == "negative_volume":
        event.data_volume_mb = -abs(event.data_volume_mb) - 0.01
    return event


def _apply_schema_drift(event: NetworkEvent) -> NetworkEvent:
    """Attach an undocumented field, simulating a probe software upgrade."""
    event.extra["volte_enabled"] = random.choice([True, False])
    return event


def generate_event(dirty_record_rate: float = 0.05, schema_drift_rate: float = 0.02) -> dict:
    """Build one synthetic network event as a plain dict, ready to serialize."""
    subscribers, cells = _get_pools()
    cell = random.choice(cells)
    event_type = random.choices(list(EventType), weights=[0.35, 0.50, 0.15])[0]

    event = NetworkEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        event_time=datetime.now(timezone.utc).isoformat(),
        # copy: dirtiness mutates the subscriber and must never leak into the shared pool
        subscriber=replace(random.choice(subscribers)),
        cell=cell,
        signal_dbm=_signal_dbm(cell),
    )
    if event_type == EventType.VOICE_CALL:
        _fill_voice(event)
    elif event_type == EventType.DATA_SESSION:
        _fill_data(event)

    if random.random() < dirty_record_rate:
        event = _apply_dirtiness(event)
    if random.random() < schema_drift_rate:
        event = _apply_schema_drift(event)

    return _event_to_dict(event)


def _event_to_dict(event: NetworkEvent) -> dict:
    cell = asdict(event.cell)
    cell.pop("degraded")  # hidden ground truth stays inside the generator
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "event_time": event.event_time,
        "signal_dbm": event.signal_dbm,
        "duration_sec": event.duration_sec,
        "call_result": event.call_result,
        "data_volume_mb": event.data_volume_mb,
        "avg_throughput_mbps": event.avg_throughput_mbps,
        "latency_ms": event.latency_ms,
        "subscriber": asdict(event.subscriber),
        "cell": cell,
        **event.extra,
    }
