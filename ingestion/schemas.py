"""
Entity shapes produced by the synthetic generator.

The model is a deliberately reduced slice of a mobile operator's network data:
subscribers, cell sites, and usage/quality events (voice calls, data sessions, SMS)
as they would appear in probe / CDR-style records. Phase 0 only needs these shapes
to exist so the producer and consumer skeletons have something concrete to import;
no transformation logic lives here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EventType(str, Enum):
    VOICE_CALL = "voice_call"
    DATA_SESSION = "data_session"
    SMS = "sms"


@dataclass
class Subscriber:
    subscriber_id: str | None
    msisdn_masked: str
    plan: str  # prepaid | postpaid | corporate
    activation_date: str  # ISO date


@dataclass
class CellSite:
    cell_id: str
    region: str
    radio_tech: str  # 3G | 4G | 5G
    degraded: bool = False  # generator-internal ground truth; NEVER emitted in events


@dataclass
class NetworkEvent:
    """
    One raw usage/quality event.

    `extra` carries schema-drift fields (new attributes that appear over time, e.g.
    after a probe software upgrade) so downstream staging has a documented place to
    expect the unexpected.
    """

    event_id: str
    event_type: EventType
    event_time: str
    subscriber: Subscriber
    cell: CellSite
    signal_dbm: float | None = None
    duration_sec: int | None = None
    call_result: str | None = None  # completed | dropped | failed_setup (voice only)
    data_volume_mb: float | None = None
    avg_throughput_mbps: float | None = None
    latency_ms: float | None = None
    extra: dict = field(default_factory=dict)
