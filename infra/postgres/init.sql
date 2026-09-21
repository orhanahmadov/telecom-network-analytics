-- Warehouse Postgres. Runs ONCE on first container start
-- (docker-entrypoint-initdb.d) - it is skipped when the data volume already exists.
--
-- Layers (see docs/PROJECT_PLAN.md, section 5):
--   raw      - as-landed data loaded from RustFS, no cleaning
--   staging  - cleaned/typed 1:1 views (dbt)
--   curated  - dimensional model + aggregates for serving (dbt + Spark)

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

-- The raw landing table is created here (not by dbt) because it is a LOAD target
-- owned by the Airflow load task in Phase 1, i.e. a contract between ingestion and
-- transformation. Columns mirror source.network_events + load metadata.
-- event_id is the primary key so the loader can use ON CONFLICT DO NOTHING
-- (idempotent re-loads).
CREATE TABLE IF NOT EXISTS raw.network_events (
    event_id                    UUID PRIMARY KEY,
    event_type                  TEXT NOT NULL,
    event_time_raw              TEXT NOT NULL,
    subscriber_id               UUID,
    subscriber_msisdn_masked    TEXT,
    subscriber_plan             TEXT,
    subscriber_activation_date  DATE,
    cell_id                     TEXT NOT NULL,
    cell_region                 TEXT,
    cell_radio_tech             TEXT,
    signal_dbm                  NUMERIC(6,1),
    duration_sec                INTEGER,
    call_result                 TEXT,
    data_volume_mb              NUMERIC(12,2),
    avg_throughput_mbps         NUMERIC(8,2),
    latency_ms                  NUMERIC(8,1),
    extra                       JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- load metadata
    _source_object_key          TEXT,
    _cdc_source_ts_ms           BIGINT,   -- source-side change time (ms) reported by Debezium; lets Phase 4 measure end-to-end latency
    _loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
