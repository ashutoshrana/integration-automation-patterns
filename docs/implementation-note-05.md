# Implementation Note 05 — CQRS in Enterprise Integration: When to Separate Reads from Writes

**Repo:** `integration-automation-patterns`
**Relates to:** [`examples/07_cqrs_event_sourcing.py`](../examples/07_cqrs_event_sourcing.py)
**ADR cross-reference:** [ADR-003 (orchestration vs. integration boundaries)](adr/003-orchestration-vs-integration-boundaries.md)

---

## What CQRS actually solves — and what it does not

Command Query Responsibility Segregation (CQRS) is one of those patterns that is simultaneously over-applied and under-applied in enterprise systems.

**Over-applied:** Teams apply CQRS to every microservice on the grounds that "writes and reads might need to scale differently." For a service where writes and reads come from the same bounded context, have similar throughput characteristics, and touch the same data, CQRS adds complexity without benefit.

**Under-applied:** CQRS is not applied to the specific systems that genuinely need it — high-volume event-driven integrations where the write model (append-only event log) and the read model (denormalized projection) have fundamentally different shape requirements.

The decision criterion: **Apply CQRS when the shape of the data you need to write and the shape of the data you need to query are so different that forcing them into a single model means either the write path or the read path must do expensive transformations.**

For a CRM opportunity pipeline:
- **Write shape:** Granular domain events (`OpportunityCreated`, `StageUpdated`, `CloseWon`) with complete provenance — who changed what, when, and why.
- **Read shape:** Denormalized summaries aggregated by stage, owner, region, or time period — pre-joined for sub-millisecond query response.

These are different enough that CQRS pays for itself.

---

## Event sourcing: why state is a function of events

In a conventional CRM, when a deal moves from "Negotiation" to "Closed Won," the database row is updated: `UPDATE opportunity SET stage = 'Closed Won' WHERE id = ?`. The previous stage is lost unless an audit trigger writes it to a separate table.

In an event-sourced system, the event is the primary artifact: `OpportunityClosedWon(opportunity_id, final_amount, close_reason, occurred_at)` is appended to the stream. The "current state" is computed by replaying all events:

```python
state = _reconstruct_opportunity(store.get_events(opportunity_id))
```

This unlocks capabilities that are impossible with mutable state:

1. **Temporal queries** — reconstruct the state at any past timestamp by replaying events up to that point. "What was the pipeline forecast as of March 1?" is answered by replaying events that occurred before March 1.

2. **Causal replay** — when a bug is found in state computation logic, fix the projection code and replay all events to get the correct state. No data migration required.

3. **Audit by construction** — the event log is the audit trail. You do not need a separate `_audit_log` table; the events themselves are the immutable record of every change.

4. **Multiple projections from one log** — the same event stream can be projected into different read models optimized for different query patterns: a stage-funnel projection, an owner-performance projection, a regional-revenue projection — all from the same append-only event log.

---

## The CommandBus / QueryBus separation at the code level

The CQRS contract — "writes go through the command path; reads go through the query path" — is most durable when it is enforced structurally rather than by convention.

In `07_cqrs_event_sourcing.py`, `CommandBus` has a reference to `EventStore` but no reference to `OpportunityProjection`. `QueryBus` has a reference to `OpportunityProjection` but no reference to `EventStore`. There is no code path through which a query can accidentally trigger a write, or a write can accidentally read a stale projection.

This structural separation has an important secondary benefit: it makes the write model and the read model independently deployable. In a microservices context, the write service (command handlers + event store) and the read service (projection + query handlers) can be scaled, deployed, and versioned independently.

---

## Optimistic concurrency: the expected_version contract

In a concurrent system, two agents may try to update the same aggregate simultaneously. Without a concurrency check, both updates would succeed and one would overwrite the other — the classic lost-update problem.

The `EventStore.append(aggregate_id, events, expected_version)` contract handles this:

```python
# Thread A reads current state at version 3, prepares an update
state = reconstruct(store.get_events(opp_id))  # version 3
events = [StageUpdated(opp_id, version=4, new_stage="Negotiation")]

# Thread B also reads version 3 and submits first
store.append(opp_id, [other_event], expected_version=3)  # succeeds → now at version 4

# Thread A now tries to append with expected_version=3 — rejected
store.append(opp_id, events, expected_version=3)  # raises ConcurrencyConflictError
```

The caller retries: reload the current state (now at version 4), recompute the business logic, and resubmit with `expected_version=4`. This is standard optimistic concurrency — it assumes conflicts are rare and handles them by retry rather than by locking.

**When to use expected_version explicitly vs. implicitly:** The `UpdateStage` command handler in `07_cqrs_event_sourcing.py` defaults `expected_version` to the current version if not provided by the caller:

```python
expected = cmd.get("expected_version", current_ver)
```

This is the safe default for single-user workflows. For workflows with concurrent writers, callers should provide the `expected_version` they loaded when they fetched the aggregate — this makes the concurrency guarantee explicit and testable.

---

## Read model rebuild: the zero-data-loss guarantee

The event log is the system of truth. The read model is a cache. Caches can be invalidated.

`OpportunityProjection.rebuild(all_events)` drops all current read model state and replays the full event log from the beginning. The resulting state is always identical to the state that would have been accumulated by processing events one by one.

This guarantee enables several operational patterns that are otherwise very expensive:

- **Schema migration:** add a new field to the read model (e.g., `days_in_stage` derived from stage transition timestamps). Rebuild the projection and the new field is populated historically.
- **Bug fix:** if the projection code had a bug that computed `total_pipeline_value` incorrectly, fix the code and rebuild. Historical data is corrected automatically.
- **New read model:** add a `RegionalPipelineProjection` that groups opportunities by account geography. Replay all events into the new projection — no ETL job needed.

The operational cost of rebuild is bounded by the total number of events in the store. For high-volume systems, snapshots (periodic checkpoints of aggregate state at a given version) reduce replay time by starting from the most recent snapshot rather than from event 0.

---

## When NOT to use CQRS + event sourcing

CQRS and event sourcing solve specific problems. Applied to the wrong context, they add overhead without value.

**Do not use event sourcing for:**

- **Configuration data** — a feature flag's current value is all that matters; its history is irrelevant to every consumer.
- **Reference data** — a country code table, a currency table. These are lookups, not aggregates with business behavior.
- **Reporting aggregates** — if you are building a system where the primary consumer is a BI tool that reads denormalized rows, use a relational database with a well-indexed schema.
- **Simple CRUD services** — a service that creates and retrieves user profiles with no complex state transitions does not benefit from event sourcing. The overhead of command handlers, event stores, and projections is real; the benefit is zero.

**The right question:** does this system have a meaningful history that consumers need to query, reason about, or replay? If the answer is "only the current state matters," use a conventional database.

---

## Integration with the outbox pattern

Event-sourced systems pair naturally with the transactional outbox pattern (see `examples/06_end_to_end_integration.py` and `implementation-note-04.md`).

When the `EventStore.append()` call succeeds, the new events need to be published to external consumers (read model projectors in other services, downstream saga triggers, analytics pipelines). The safest pattern is:

1. Append domain events to the event store **in the same database transaction** as inserting outbox records for each event.
2. The `OutboxProcessor` polls the outbox and publishes events to the message broker with at-least-once delivery.
3. Each consumer deduplicates by `event_id` before updating its own read model.

This avoids the dual-write problem: either both the event store append and the outbox insert succeed together, or neither does. There is no window where an event is in the store but not queued for publication — or vice versa.
