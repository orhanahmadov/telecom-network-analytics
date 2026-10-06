-- Quarantined half of the split: every row carrying at least one dq_flag. Kept, not
-- dropped, so a corrupted batch is auditable instead of silently vanishing
-- (docs/PROJECT_PLAN.md section 7).
select *
from {{ ref('stg_network_events') }}
where array_length(dq_flags, 1) is not null
