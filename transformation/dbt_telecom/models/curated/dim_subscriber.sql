-- SCD Type 2 on `plan` (docs/PROJECT_PLAN.md section 5): one row per subscriber per
-- validity period, keyed on (subscriber_id, valid_from). A prepaid -> postpaid migration
-- must not rewrite history, since churn analysis needs the plan as-of the event.
--
-- There is no separate subscriber master table anywhere upstream - `plan` is only ever
-- observed on an event - so this derives version history directly from the event stream
-- instead of from a dbt snapshot. See docs/decisions/0001-derive-dim-subscriber-scd2-from-event-stream.md
-- for why, including the current limitation: the generator does not simulate plan changes
-- yet (docs/PROJECT_PLAN.md section 8, Phase 1 scope), so today every subscriber has
-- exactly one version. The logic below is still correct, general-purpose SCD2 and needs
-- no changes once the generator starts emitting migrations.
with observed as (

    select
        event_id,
        subscriber_id,
        subscriber_msisdn_masked,
        subscriber_plan,
        subscriber_activation_date,
        event_time
    from {{ ref('stg_network_events_valid') }}
    where subscriber_id is not null

),

with_previous_plan as (

    select
        *,
        lag(subscriber_plan) over (
            partition by subscriber_id order by event_time, event_id
        ) as previous_plan
    from observed

),

versioned as (

    select
        *,
        sum(case when previous_plan is distinct from subscriber_plan then 1 else 0 end) over (
            partition by subscriber_id
            order by event_time, event_id
            rows between unbounded preceding and current row
        ) as version_number
    from with_previous_plan

),

versions as (

    select
        subscriber_id,
        max(subscriber_msisdn_masked) as subscriber_msisdn_masked,
        subscriber_plan as plan,
        max(subscriber_activation_date) as subscriber_activation_date,
        version_number,
        min(event_time) as valid_from
    from versioned
    group by subscriber_id, subscriber_plan, version_number

)

select
    subscriber_id,
    subscriber_msisdn_masked,
    plan,
    subscriber_activation_date,
    valid_from,
    lead(valid_from) over (partition by subscriber_id order by version_number) as valid_to,
    lead(valid_from) over (partition by subscriber_id order by version_number) is null as is_current
from versions
