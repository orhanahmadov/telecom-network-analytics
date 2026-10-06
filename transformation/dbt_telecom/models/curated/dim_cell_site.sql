-- SCD Type 1 (overwrite) - region/technology are stable enough that history isn't
-- analytically useful (docs/PROJECT_PLAN.md section 5). The curated schema's default
-- materialization is `table` (see dbt_project.yml), so every `dbt run` fully recomputes
-- this as "latest observed per cell", which IS Type 1 overwrite semantics - no merge
-- logic needed.
select distinct on (cell_id)
    cell_id,
    cell_region,
    cell_radio_tech,
    _loaded_at as last_seen_at
from {{ ref('stg_network_events_valid') }}
order by cell_id, _loaded_at desc
