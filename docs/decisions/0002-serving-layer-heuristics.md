# 0002 - Serving-layer heuristics for the two business questions

## Status

Accepted (Phase 1). Thresholds and weights below are initial assumptions, explicitly
flagged in docs/PROJECT_PLAN.md section 5 as "tuned in Phase 5" once real stakeholder
feedback or labeled churn outcomes are available.

## Context

docs/PROJECT_PLAN.md section 1 names two business questions the serving layer must
answer with a single query each:

1. **Network Operations**: which cells need attention right now?
2. **Retention**: which subscribers show early churn signals right now?

Section 5 names the two views (`vw_cell_quality_ranking`, `vw_subscriber_churn_signals`)
and gives a starting heuristic for churn, but several implementation choices were left
open and had to be decided to write the SQL.

## Decisions

**"Now" is data-defined, not wall-clock.** Both views anchor their trailing windows on
`max(event_time)` across the curated fact tables, not `now()`. This pipeline replays a
simulated historical event stream on a logical date, so wall-clock time has no
relationship to the data's own timeline; anchoring on the latest event keeps the views
reproducible for any logical date the pipeline has processed.

**Cell quality score** (`vw_cell_quality_ranking`) combines three signals into one
"higher = worse" number over a trailing 7-day window:

```
quality_score = drop_rate * 100 + avg_latency_ms / 10 - avg_throughput_mbps / 10
```

Drop rate is weighted heaviest (it is the clearest, most direct network-ops signal from
docs/PROJECT_PLAN.md section 1 - "drop rate, throughput and latency"); latency and
(inverse) throughput are secondary. The divisors keep latency/throughput, which are in
the tens-to-hundreds range, from swamping a drop rate that is a 0-1 fraction. Cells with
no voice or data traffic in the window are excluded rather than scored as perfect.

**Subscriber "activity" proxy** (`vw_subscriber_churn_signals`) is the count of
voice_call + data_session events, not a combination of minutes and megabytes. Section 5's
heuristic talks about "usage" without defining units, and voice (`duration_sec`) and data
(`data_volume_mb`) use incompatible units; inventing a conversion factor (e.g. "1 MB ~= 1
minute") would be an arbitrary additional assumption. Event count is unitless, available
for both event types, and a reasonable first-pass proxy for "how active is this
subscriber" - revisit once there's a labeled outcome to validate against.

**Usage-drop and poor-experience flags** follow section 5's heuristic literally:
usage-drop when trailing-7-day activity <= 60% of the trailing 4-week weekly average
(computed over the 7-35 days window, i.e. the four weeks immediately before the recent
7-day window, averaged per week); poor-experience when there are >= 3 events in the last
14 days that are either a dropped/failed voice call, or a data session with latency above
100ms on a 4G/5G cell (the "poor-experience" definition from section 5, restated using
the generator's actual field spellings - `call_result` in `('dropped', 'failed_setup')`,
`cell_radio_tech` in `('4G', '5G')`). A subscriber with zero activity in the prior 4 weeks
is excluded from the usage-drop flag (nothing to compare against) rather than flagged by
default.

**Serving views join `dim_subscriber` on `is_current`** so a subscriber's current plan
is shown, consistent with every other reference to `dim_subscriber` as a
point-in-time-aware SCD2 table (docs/decisions/0001).

## Consequences

Both views are plain `select`s over `curated` (materialized as dbt views), consistent
with docs/PROJECT_PLAN.md section 3's "no extra service is needed" - any SQL client can
query them directly, and they recompute from scratch on every `dbt run`, so they are
always current with no manual work, satisfying the Phase 5 success criteria in section 1.

The specific weights, window lengths, and the 60%/3-events thresholds are not derived
from any labeled ground truth - they encode the plan's stated heuristic as literally as
possible and are expected to be retuned once real signal (ops feedback, actual churn
outcomes) is available.
