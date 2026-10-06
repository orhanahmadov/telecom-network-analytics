-- Clean half of the split (check table, docs/PROJECT_PLAN.md section 7): no dq_flags at all.
-- This is what curated is built from.
select *
from {{ ref('stg_network_events') }}
where array_length(dq_flags, 1) is null
