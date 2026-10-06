-- Retention question (docs/PROJECT_PLAN.md, section 1 and 5): which subscribers show
-- early churn signals right now? Flags a subscriber when BOTH hold, anchored on the
-- latest event in curated data (see vw_cell_quality_ranking for why "now" is
-- data-defined, not wall-clock):
--   (a) last-7-day activity is at most 60% of their trailing 4-week weekly average, and
--   (b) they had at least 3 poor-experience events in the last 14 days (a dropped/
--       failed voice call, or a 4G/5G data session with latency above 100ms).
-- "Activity" = count of voice_call + data_session events, a unitless usage proxy chosen
-- over inventing a conversion rate between MB and seconds - see ADR 0002. Thresholds are
-- initial assumptions, tuned in Phase 5.
with reference_time as (

    select max(event_time) as ts
    from (
        select event_time from {{ ref('fct_voice_call') }}
        union all
        select event_time from {{ ref('fct_data_session') }}
    ) all_events

),

subscriber_events as (

    select
        subscriber_id,
        event_time,
        cell_id,
        call_result,
        null::numeric as latency_ms
    from {{ ref('fct_voice_call') }}
    where subscriber_id is not null

    union all

    select
        subscriber_id,
        event_time,
        cell_id,
        null as call_result,
        latency_ms
    from {{ ref('fct_data_session') }}
    where subscriber_id is not null

),

activity as (

    select
        e.subscriber_id,
        count(*) filter (
            where e.event_time > r.ts - interval '7 days' and e.event_time <= r.ts
        ) as last_7_day_activity,
        count(*) filter (
            where e.event_time > r.ts - interval '35 days'
              and e.event_time <= r.ts - interval '7 days'
        ) / 4.0 as trailing_4_week_weekly_avg_activity
    from subscriber_events e
    cross join reference_time r
    group by e.subscriber_id

),

poor_experience as (

    select
        e.subscriber_id,
        count(*) as poor_experience_count
    from subscriber_events e
    cross join reference_time r
    left join {{ ref('dim_cell_site') }} c on c.cell_id = e.cell_id
    where e.event_time > r.ts - interval '14 days'
      and e.event_time <= r.ts
      and (
          e.call_result in ('dropped', 'failed_setup')
          or (
              e.latency_ms is not null
              and e.latency_ms > 100
              and c.cell_radio_tech in ('4G', '5G')
          )
      )
    group by e.subscriber_id

),

signals as (

    select
        a.subscriber_id,
        a.last_7_day_activity,
        round(a.trailing_4_week_weekly_avg_activity, 2) as trailing_4_week_weekly_avg_activity,
        coalesce(p.poor_experience_count, 0) as poor_experience_count_14d,
        (
            a.trailing_4_week_weekly_avg_activity > 0
            and a.last_7_day_activity <= 0.6 * a.trailing_4_week_weekly_avg_activity
        ) as usage_drop_flag,
        coalesce(p.poor_experience_count, 0) >= 3 as poor_experience_flag
    from activity a
    left join poor_experience p on p.subscriber_id = a.subscriber_id

)

select
    s.subscriber_id,
    sub.plan,
    s.last_7_day_activity,
    s.trailing_4_week_weekly_avg_activity,
    s.poor_experience_count_14d,
    s.usage_drop_flag,
    s.poor_experience_flag
from signals s
inner join {{ ref('dim_subscriber') }} sub
    on sub.subscriber_id = s.subscriber_id and sub.is_current
where s.usage_drop_flag and s.poor_experience_flag
order by s.poor_experience_count_14d desc, s.last_7_day_activity asc
