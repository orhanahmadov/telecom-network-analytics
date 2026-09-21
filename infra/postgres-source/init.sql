-- Source-side OLTP table the synthetic generator writes into.
-- Debezium's PostgreSQL connector reads this database's WAL via logical
-- replication (wal_level=logical, set in infra/docker-compose.yml) and streams
-- every INSERT to Kafka - no application code publishes to Kafka directly.
--
-- Deliberate "dirty data" affordances:
--   * event_time_raw is TEXT: a malformed timestamp must be storable, not rejected.
--   * subscriber_id is nullable; numeric columns accept negative / impossible values.
--   * extra is a catch-all JSONB column for schema-drift attributes
--     (e.g. volte_enabled) so new fields never require a table migration.
--
-- Column list must stay in sync with ingestion/producer/db_writer.py
-- (enforced by tests/ingestion/test_db_writer.py).

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.network_events (
    event_id                    UUID PRIMARY KEY,
    event_type                  TEXT NOT NULL,     -- voice_call | data_session | sms
    event_time_raw              TEXT NOT NULL,
    subscriber_id               UUID,
    subscriber_msisdn_masked    TEXT,
    subscriber_plan             TEXT,              -- prepaid | postpaid | corporate
    subscriber_activation_date  DATE,
    cell_id                     TEXT NOT NULL,
    cell_region                 TEXT,
    cell_radio_tech             TEXT,              -- 3G | 4G | 5G
    signal_dbm                  NUMERIC(6,1),      -- RSRP-like, valid range about -140..-44
    duration_sec                INTEGER,
    call_result                 TEXT,              -- completed | dropped | failed_setup (voice only)
    data_volume_mb              NUMERIC(12,2),
    avg_throughput_mbps         NUMERIC(8,2),
    latency_ms                  NUMERIC(8,1),
    extra                       JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);
