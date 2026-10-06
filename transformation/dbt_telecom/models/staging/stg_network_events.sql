-- Typed, flagged 1:1 view over raw.network_events (see docs/PROJECT_PLAN.md, sections 5 and 7).
--
-- Deduplication is NOT done here: raw.network_events has event_id as its primary key and the
-- loader inserts with ON CONFLICT (event_id) DO NOTHING (ingestion/loader/raw_loader.py), so
-- every event_id is already unique by the time it reaches this view.
--
-- Bad rows are flagged, never dropped: dq_flags is a Postgres array of zero or more of
-- {bad_timestamp, missing_subscriber, negative_duration, negative_volume, signal_out_of_range}.
-- stg_network_events_valid / stg_network_events_rejected (below) split on it.
with casted as (

    select
        event_id,
        event_type,
        -- event_time_raw is corrupted to the literal string "not-a-timestamp" for a
        -- dirty_record_rate share of events (ingestion/producer/generator.py); the regex
        -- guards the cast so a bad value becomes NULL instead of failing the whole query.
        case
            when event_time_raw ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
                then event_time_raw::timestamptz
        end as event_time,
        subscriber_id,
        subscriber_msisdn_masked,
        subscriber_plan,
        subscriber_activation_date,
        cell_id,
        cell_region,
        cell_radio_tech,
        signal_dbm as signal_dbm_raw,
        duration_sec,
        call_result,
        data_volume_mb,
        avg_throughput_mbps,
        latency_ms,
        -- Known schema-drift field (check 8): pulled out of `extra` into a typed column.
        -- Anything else that ever lands in `extra` stays there, untyped, for later discovery.
        (extra ->> 'volte_enabled')::boolean as volte_enabled,
        extra,
        _source_object_key,
        _cdc_source_ts_ms,
        _loaded_at
    from {{ source('raw', 'network_events') }}

),

flagged as (

    select
        *,
        array_remove(array[
            case when event_time is null
                then 'bad_timestamp' end,
            case when subscriber_id is null
                then 'missing_subscriber' end,
            case when duration_sec is not null and duration_sec < 0
                then 'negative_duration' end,
            case when data_volume_mb is not null and data_volume_mb < 0
                then 'negative_volume' end,
            case when signal_dbm_raw is not null and (signal_dbm_raw < -140 or signal_dbm_raw > -44)
                then 'signal_out_of_range' end
        ], null) as dq_flags
    from casted

)

select
    event_id,
    event_type,
    event_time,
    subscriber_id,
    subscriber_msisdn_masked,
    subscriber_plan,
    subscriber_activation_date,
    cell_id,
    cell_region,
    cell_radio_tech,
    -- check 5: an out-of-range signal is kept flagged but nulled out - the rest of the
    -- event (call result, duration, volume...) is still perfectly usable.
    case when 'signal_out_of_range' = any(dq_flags) then null else signal_dbm_raw end as signal_dbm,
    duration_sec,
    call_result,
    data_volume_mb,
    avg_throughput_mbps,
    latency_ms,
    volte_enabled,
    extra,
    dq_flags,
    _source_object_key,
    _cdc_source_ts_ms,
    _loaded_at
from flagged
