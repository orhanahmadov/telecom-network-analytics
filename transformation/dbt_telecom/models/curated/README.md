# Curated layer (dimensions and facts)

Intentionally empty in Phase 0 - no business logic or transformations belong in this
phase. Planned models (see `docs/PROJECT_PLAN.md`, section 5):

- `dim_subscriber` (SCD Type 2 on plan)
- `dim_cell_site` (SCD Type 1)
- `fct_voice_call`
- `fct_data_session`

Added in Phase 2 (Transformation), after Phase 1 fills the raw layer.

`agg_cell_hourly_kpi` is **not** built by dbt: it is owned by the Spark batch job at
`transformation/spark_jobs/cell_hourly_kpi_batch.py`, which reads raw NDJSON directly
from RustFS and writes the aggregate into this same `curated` schema via JDBC.
See `docs/PROJECT_PLAN.md` section 3 for the reasoning behind that split.
