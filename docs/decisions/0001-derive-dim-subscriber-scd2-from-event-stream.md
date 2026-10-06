# 1. Derive `dim_subscriber`'s SCD Type 2 history from the event stream, not a snapshot

Date: 2026-10-06
Status: Accepted

## Context

`docs/PROJECT_PLAN.md` (section 5) specifies `dim_subscriber` as SCD Type 2 on `plan`: a
prepaid -> postpaid migration must produce a new validity-period row, not overwrite history,
because churn analysis needs the plan *as of* the event.

dbt's standard tool for SCD Type 2 is a **snapshot**: it compares a source table's current
state against the last snapshot run and appends a new version row when a tracked column
changes. That pattern assumes a queryable "current state" table to snapshot - e.g. a
`subscribers` master table with one current row per subscriber.

This project has no such table. `source.network_events` (and therefore `raw`/`staging`) is
a pure event log: `subscriber_plan` is an attribute carried on every event, not stored
anywhere as a standalone "current plan" record. There is nothing to snapshot.

## Decision

`dim_subscriber.sql` derives version history directly from `stg_network_events_valid`
using window functions:

1. For each subscriber, look at events in time order and detect wherever the observed
   `plan` differs from the previous event's `plan` (`LAG`).
2. Number those "plan version" boundaries per subscriber (`SUM(...) OVER (...)`).
3. Group by `(subscriber_id, version_number)` to get one row per validity period, with
   `valid_from = MIN(event_time)` in that version.
4. `valid_to` is the next version's `valid_from` (`LEAD`); `NULL` (and `is_current = true`)
   for a subscriber's latest version.

This is full-refresh (`curated` models default to `materialized: table`), recomputed from
the complete valid event history on every `dbt run` - correct and simple, at the cost of
rescanning all of `stg_network_events_valid` each time. That cost is acceptable at this
project's scale (Section 1: ~432k events/day) and is the same trade-off `dim_cell_site`
already makes for its own (simpler) SCD Type 1 overwrite.

## Consequences

- No dbt snapshot, no extra snapshot-state table to manage.
- The generator does not yet simulate plan migrations
  (`docs/PROJECT_PLAN.md`, section 8, Phase 1 scope: "generator gains slow plan
  migrations... so SCD2 has something to track"). Until it does, every subscriber will
  have exactly **one** version in `dim_subscriber` - the logic is correct but currently
  unexercised by multi-version data. No model change will be needed once the generator
  adds migrations; it is purely a test-data gap, tracked separately.
- A subscriber's `subscriber_msisdn_masked` and `subscriber_activation_date` are assumed
  constant across versions (only `plan` is versioned); the model takes `MAX(...)` of each
  per version, which is correct as long as that assumption holds in the generator.
