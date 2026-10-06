-- Incremental, unique_key = event_id, watermark on event_time (docs/PROJECT_PLAN.md
-- section 5). Only valid (unflagged) rows are published - see stg_network_events_valid.
{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

select
    event_id,
    event_time,
    subscriber_id,
    cell_id,
    call_result,
    duration_sec,
    signal_dbm
from {{ ref('stg_network_events_valid') }}
where event_type = 'voice_call'

{% if is_incremental() %}
    and event_time > (select coalesce(max(event_time), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}
