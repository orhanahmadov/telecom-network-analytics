# Curated layer (dimensions and facts)

Built from `stg_network_events_valid` (see `../staging/`) - only rows with zero dq_flags
reach here. See `docs/PROJECT_PLAN.md`, section 5, for the full design.

- `dim_cell_site.sql` - one row per cell, current attributes only (SCD Type 1 overwrite).
- `dim_subscriber.sql` - one row per subscriber per validity period (SCD Type 2 on `plan`),
  derived from the event stream itself - see
  `docs/decisions/0001-derive-dim-subscriber-scd2-from-event-stream.md`.
- `fct_voice_call.sql` / `fct_data_session.sql` - incremental, `unique_key = event_id`,
  watermarked on `event_time`.

`agg_cell_hourly_kpi` is **not** built by dbt: it is owned by the Spark batch job at
`transformation/spark_jobs/cell_hourly_kpi_batch.py`, which reads raw NDJSON directly
from RustFS and writes the aggregate into this same `curated` schema via JDBC
(still a Phase 3 placeholder - see `docs/PROJECT_PLAN.md` section 8).
