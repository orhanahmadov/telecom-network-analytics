-- Phase 0 placeholder: 1:1 passthrough from the raw source, no cleaning or type casting.
-- Real staging logic (dedup, timestamp parsing, range checks, drift extraction from
-- `extra`) is scoped for Phase 2 per docs/PROJECT_PLAN.md.
select *
from {{ source('raw', 'network_events') }}
