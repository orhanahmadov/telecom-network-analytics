-- Network Operations question (docs/PROJECT_PLAN.md, section 1 and 5): which cells need
-- attention right now? Ranks cells by a composite "higher = worse" quality score over a
-- trailing 7-day window anchored on the latest event in curated data, not wall-clock
-- now() - this is a batch pipeline replaying a simulated historical stream, so "now" is
-- defined by the data itself. Weights/window are initial assumptions tuned later in
-- Phase 5 - see docs/decisions/0002-serving-layer-heuristics.md.
with reference_time as (

    select max(event_time) as ts
    from (
        select event_time from {{ ref('fct_voice_call') }}
        union all
        select event_time from {{ ref('fct_data_session') }}
    ) all_events

),

voice_agg as (

    select
        v.cell_id,
        count(*) as voice_call_count,
        sum(case when v.call_result in ('dropped', 'failed_setup') then 1 else 0 end)
            as dropped_or_failed_count
    from {{ ref('fct_voice_call') }} v
    cross join reference_time r
    where v.event_time > r.ts - interval '7 days'
      and v.event_time <= r.ts
    group by v.cell_id

),

data_agg as (

    select
        d.cell_id,
        count(*) as data_session_count,
        avg(d.avg_throughput_mbps) as avg_throughput_mbps,
        avg(d.latency_ms) as avg_latency_ms
    from {{ ref('fct_data_session') }} d
    cross join reference_time r
    where d.event_time > r.ts - interval '7 days'
      and d.event_time <= r.ts
    group by d.cell_id

),

cell_metrics as (

    select
        c.cell_id,
        c.cell_region,
        c.cell_radio_tech,
        coalesce(v.voice_call_count, 0) as voice_call_count,
        coalesce(v.dropped_or_failed_count, 0) as dropped_or_failed_count,
        case
            when coalesce(v.voice_call_count, 0) = 0 then null
            else v.dropped_or_failed_count::numeric / v.voice_call_count
        end as drop_rate,
        coalesce(d.data_session_count, 0) as data_session_count,
        d.avg_throughput_mbps,
        d.avg_latency_ms
    from {{ ref('dim_cell_site') }} c
    left join voice_agg v on v.cell_id = c.cell_id
    left join data_agg d on d.cell_id = c.cell_id

),

scored as (

    select
        *,
        -- Weighted toward drop rate (the clearest network-ops signal), with latency and
        -- (inverse) throughput as secondary signals. See ADR 0002 for the weights.
        (coalesce(drop_rate, 0) * 100)
            + (coalesce(avg_latency_ms, 0) / 10.0)
            - (coalesce(avg_throughput_mbps, 0) / 10.0) as quality_score
    from cell_metrics
    where voice_call_count > 0 or data_session_count > 0

)

select
    cell_id,
    cell_region,
    cell_radio_tech,
    voice_call_count,
    dropped_or_failed_count,
    round(drop_rate, 4) as drop_rate,
    data_session_count,
    round(avg_throughput_mbps, 2) as avg_throughput_mbps,
    round(avg_latency_ms, 2) as avg_latency_ms,
    round(quality_score, 4) as quality_score,
    rank() over (order by quality_score desc) as quality_rank
from scored
order by quality_rank
