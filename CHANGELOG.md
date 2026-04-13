# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.18.0] — 2026-04-13

### Added — Workflow Orchestration Patterns (State Machine + Activity Execution + Compensation + Retry)

**`examples/19_workflow_orchestration_patterns.py`** — workflow orchestration patterns for
long-running business processes with saga-style compensation, configurable activity retry,
and strict state machine enforcement. Inspired by Temporal, Apache Conductor, and Camunda
workflow engines; implemented with zero external dependencies.

**New classes:**
- `WorkflowState` — 7-state enum: PENDING / RUNNING / COMPLETED / FAILED /
  COMPENSATING / COMPENSATED / CANCELLED; terminal states reject all transitions
- `WorkflowTransitionError` — raised on invalid state transitions
- `WorkflowTimeoutError` — raised when execution exceeds `timeout_seconds`
- `ActivityFailedError` — raised when activity fails and compensation is initiated
- `ActivityResult` — activity_id, success, output, error, duration_ms, compensated flag
- `WorkflowActivity` (Protocol) — `execute(input_data)→ActivityResult`
- `CompensatingActivity` (Protocol) — `compensate(original_input, original_output)→bool`
- `WorkflowStep` — (activity, compensating_activity, max_retries, retry_delay_seconds)
- `WorkflowDefinition` — named ordered list of steps with optional timeout_seconds
- `WorkflowInstance` — stateful execution; `execute()` runs forward, triggers reverse
  compensation on failure; `transition_to()` enforces valid state machine; `get_history()`,
  `successful_steps()`, `compensated_steps()`, `elapsed_seconds()` introspection
- `WorkflowOrchestrator` — manages multiple instances; `start(definition, input)→id`;
  `get_status()`, `cancel()`, `list_active()`, `list_by_state()`, `completed_count()`,
  `failed_count()`

**Tests:** 34 tests in `tests/test_workflow_orchestration_patterns.py`

---

## [0.17.0] — 2026-04-13

### Added — Message Routing Patterns (Content-Based Router + Splitter + Aggregator + Filter)

**`examples/18_message_routing_patterns.py`** — four classic Enterprise Integration Patterns
(Hohpe & Woolf, 2003) for message routing: content-based routing with predicate dispatch,
message splitting with correlation tracking, stateful many-to-one aggregation with completion
predicates and timeout support, and message filtering with dead-letter channel routing.

**New classes:**
- `Message` — dataclass with payload, message_id (UUID), correlation_id, channel,
  headers dict, sequence, total_parts, parent_id, timestamp
- `ContentBasedRouter` — predicate-based channel routing; `add_route(channel, predicate)`
  (fluent); `route(msg)→str` (first-match-wins, returns default_channel if none match);
  `route_all(msgs)→Dict[str,List[Message]]` grouping by channel
- `MessageSplitter` — one-to-many splitting with `split_fn` callable;
  each child message gets: new message_id, correlation_id=original.message_id,
  parent_id=original.message_id, sequence=0..N-1, total_parts=N
- `AggregationTimeout` — raised by `receive_with_timeout()` when elapsed > max_wait_seconds
- `MessageAggregator` — many-to-one aggregation with completion_predicate and
  aggregation_strategy callables; per-correlation threading.Lock for thread safety;
  `receive(msg)→Optional[Message]` returns aggregated result when complete;
  `receive_with_timeout(msg)` raises AggregationTimeout; `pending_groups()→List[str]`;
  `flush_group(cid)→Optional[List[Message]]` for partial retrieval
- `MessageFilter` — predicate accept/reject with optional dead_letter_channel;
  `filter(msg)→Optional[Message]` (rejected messages routed to DLQ if configured);
  `filter_all(msgs)→(accepted,rejected)`; `accepted_count`/`rejected_count` properties;
  `reset_stats()` for counter clearing

**Tests:** 27 tests in `tests/test_message_routing_patterns.py`

---

## [0.16.0] — 2026-04-13

### Added — Resilience Patterns (Circuit Breaker + Bulkhead + Rate Limiter + Retry with Jitter)

**`examples/17_resilience_patterns.py`** — four complementary microservice resilience patterns
covering the complete fault-tolerance stack: circuit breaking for dependency failure isolation,
bulkhead thread-pool separation for concurrency containment, token bucket rate limiting with
per-identity burst capacity, and exponential backoff with full jitter for safe retry behavior.

**New classes:**
- `CircuitBreakerState` — CLOSED / OPEN / HALF_OPEN state machine
- `CircuitBreakerOpenError` — raised immediately when circuit is OPEN, treated as non-retryable
- `CircuitBreakerConfig` — failure_rate_threshold, slow_call_rate_threshold,
  slow_call_duration_threshold, minimum_calls, sliding_window_size, wait_duration_in_open_state,
  permitted_calls_in_half_open
- `SlidingWindowCircuitBreaker` — sliding deque of outcomes; evaluates failure/slow-call rate
  after minimum_calls; `_check_half_open_transition()` transitions OPEN→HALF_OPEN after wait;
  probe success → CLOSED; probe failure → OPEN; context manager support
- `BulkheadFullError` — raised when BulkheadPool capacity is exceeded
- `BulkheadPool` — fixed-concurrency isolation with `acquire()`/`release()`/`call(fn)`;
  `active_calls` and `available` properties; thread-safe; context manager support
- `TokenBucketState` — per-identity mutable token bucket (tokens + last_refill timestamp)
- `BurstAwareRateLimiter` — per-identity token bucket with configurable refill rate and burst
  capacity; `acquire(identity)` raises `RateLimitExceeded` when empty; `tokens_available(identity)`
  for inspection; thread-safe with per-limiter lock
- `RateLimitExceeded` — raised when identity has exhausted burst capacity
- `RetryConfig` — max_attempts=3, base_delay=0.1s, max_delay=30.0s, jitter_factor=1.0,
  non_retryable exception set (defaults to `{CircuitBreakerOpenError}`)
- `JitteredRetryPolicy` — `execute(fn)`: full jitter delay `random.uniform(0, min(max_delay,
  base × 2^attempt))`; CircuitBreakerOpenError → no retry; non_retryable → no retry;
  `execute_with_breaker(fn, circuit_breaker)` wraps fn in circuit breaker call

**Test coverage:** 34 tests across all four patterns, including thread-safety validation for
BulkheadPool concurrent access and BurstAwareRateLimiter concurrent acquires.

---

## [0.15.0] — 2026-04-13

### Added — Event Sourcing and CQRS Patterns (EventStore + Aggregate + Projection + Snapshot)

**`examples/16_event_sourcing_cqrs.py`** — four complementary patterns for reliable, auditable,
and eventually-consistent distributed data management covering the complete event-sourcing
lifecycle from command to queryable read model.

**New classes:**
- `DomainEvent` (frozen dataclass) — immutable event record with aggregate_id, event_type,
  payload, sequence number, and UUID event_id
- `ExpectedVersion` — optimistic concurrency sentinel values: ANY (-1) skips check,
  NO_STREAM (0) asserts new stream, integer N asserts exact event count
- `ConcurrencyError` — raised by EventStore.append on optimistic concurrency conflict;
  caller should reload and retry
- `EventStore` — append-only, thread-safe per-aggregate event streams with global stream
  for projection replay; per-stream locking prevents interleaved concurrent writes
- `Aggregate` (base class) — `_raise_event` increments version + dispatches to
  `apply_<event_type>` + buffers uncommitted; `load_from_history` replays without buffering
- `OrderAggregate` — sample aggregate with four commands (place/pay/ship/cancel) and
  corresponding apply handlers enforcing business invariants
- `CommandResult` (frozen) — success flag, aggregate_id, events_saved, error string
- `CommandHandler` — CQRS write side: load aggregate → execute command → save new events
  at expected_version; catches ValueError and ConcurrencyError as CommandResult failures
- `OrderReadModel` (dataclass) — denormalized query model built from domain events
- `OrderProjection` — CQRS read side: rebuild (full replay) and update_incremental (tail
  replay from last processed position); dispatches by event_type to update read models
- `Snapshot` (dataclass) — point-in-time state capture at a given aggregate version
- `SnapshotStore` — thread-safe in-memory snapshot store (one snapshot per aggregate)
- `SnapshotCommandHandler` — extends CommandHandler; loads from snapshot + tail events;
  auto-snapshots when version reaches multiple of snapshot_every

**New tests:** 46 tests across `tests/test_event_sourcing_cqrs.py`

**Implementation note:** See `docs/implementation-note-13.md` for architectural guidance on
choosing between orchestration-based and choreography-based saga patterns when combined with
event sourcing.

---

## [0.14.0] — 2026-04-13

### Added — Distributed Saga Orchestration Patterns (Orchestration + Choreography + Outbox + DLQ)

**`examples/15_saga_orchestration_patterns.py`** — four complementary patterns that together
provide reliable distributed transaction semantics without two-phase commit, covering all major
failure modes in microservices workflows.

**New classes:**
- `SagaStatus` — RUNNING / COMPLETED / COMPENSATING / COMPENSATED / FAILED
- `SagaStepStatus` — PENDING / COMPLETED / FAILED / COMPENSATED / COMPENSATION_FAILED
- `SagaStep` (dataclass) — action + compensate callables with per-step status tracking
- `SagaExecution` (dataclass) — saga_id, shared context dict, completed_steps list, failure info
- `SagaOrchestrator` — executes steps in order; on failure triggers reverse-order compensation;
  doubles compensation timeout on cascading failures; FAILED status if compensation itself fails
- `ChoreographyEvent` (dataclass) — event_id, event_type, aggregate_id, payload, timestamp
- `ChoreographyEventBus` — thread-safe pub/sub with subscribe/unsubscribe/emit; handler exceptions
  caught without affecting other handlers; `published_events` property for test inspection
- `OutboxMessageStatus` — PENDING / PUBLISHED / FAILED
- `OutboxMessage` (dataclass) — transactional outbox entry with retry_count and published_at
- `OutboxStore` — in-memory store with save, get_pending (batch), mark_published, mark_failed,
  all_messages
- `OutboxPoller` — batch poll cycle: publisher callable → mark_published on success → mark_failed
  on exception; skips messages exceeding max_retries; returns published count
- `DLQMessageStatus` — DEAD / REPLAYED / DISCARDED
- `DLQMessage` (dataclass) — original_message_id, queue_name, payload, failure_reason, attempt_count
- `DeadLetterQueue` — enqueue, replay (handler callable + REPLAYED status + replayed_at), discard,
  list_messages (filterable by status + queue_name), size; thread-safe with per-operation locking

**Tests:** 46 new tests in `tests/test_saga_orchestration_patterns.py`

---

## [0.13.0] — 2026-04-13

### Added — API Gateway Patterns (Rate Limiting + Circuit Breaker + Coalescing + API Keys + Versioning + Idempotency)

**`examples/14_api_gateway_patterns.py`** — reference implementations of the six core patterns
every production API gateway must solve at the application layer, independent of network-layer
proxies. Covers rate limiting (two algorithms), circuit breaking with exponential backoff,
in-flight request coalescing, API key lifecycle management, version routing with sunset policy,
and client-driven idempotency.

New classes (self-contained in the example):
- `RateLimitAlgorithm` — TOKEN_BUCKET, SLIDING_WINDOW_LOG
- `RateLimitResult` — allowed, limit, remaining, reset_after_ms, retry_after_ms, algorithm, client_id
- `CircuitState` — CLOSED (normal), OPEN (failing fast), HALF_OPEN (probing)
- `CircuitBreakerResult` — allowed, state, failure_count, last_failure_at, reason
- `APIKeyStatus` — ACTIVE, EXPIRING_SOON, EXPIRED, REVOKED
- `APIKeyTier` — FREE (10 rpm), STANDARD (100 rpm), PREMIUM (1000 rpm)
- `APIKey` — key_id, key_hash (SHA-256), client_id, tier, created/expires timestamps, status,
  rate_limit_rpm; `is_valid` property; `in_grace_period` property (successor key set + within grace window)
- `APIVersion` — frozen: version_id, introduced_at, deprecated_at, sunset_at, is_current,
  handler_name; `is_deprecated` property; `is_expired` property (time-based, monotonic)
- `VersionRoutingResult` — version_id, handler_name, is_deprecated, is_expired, sunset_header, warning_header
- `IdempotencyRecord` — idempotency_key, response, created_at, expires_at, request_hash; `is_expired`
- `TokenBucketRateLimiter(client_id, rate_per_second, bucket_capacity)` — refills `_tokens` on each
  check based on elapsed time; allows burst up to `bucket_capacity`; thread-safe Lock; allow_count/deny_count
- `SlidingWindowRateLimiter(client_id, limit, window_ms)` — deque log of timestamps; prunes entries
  older than window; exact counting with no boundary artifacts; retry_after_ms = oldest - cutoff
- `CircuitBreaker(service_name, failure_threshold, success_threshold, timeout_ms, max_timeout_ms)` —
  CLOSED→OPEN on consecutive failures; HALF_OPEN probe after timeout_ms; exponential backoff on re-trip
  (doubles `_current_timeout_ms` up to `max_timeout_ms`); returns to CLOSED after success_threshold successes
- `RequestCoalescer(key_fn)` — `get_or_fetch(fetch_fn, *args, **kwargs)` → (result, was_coalesced);
  threading.Event per waiter; `_in_flight` dict prevents duplicate backend calls; coalesced_count/forwarded_count
- `APIKeyManager(rotation_warning_days, grace_period_ms, default_ttl_days)` — `create_key()` → (raw_key, APIKey);
  SHA-256 hashing; `validate_key(key_id, raw_key)` → (bool, APIKey, reason); `rotate_key()` creates successor
  + sets grace_period_ends_at on old key; `revoke_key()` transitions to REVOKED; `TIER_RATE_LIMITS` class attribute
- `APIVersionRouter` — `register_version(version)`; `route(version_header, url_prefix)` → VersionRoutingResult;
  resolution order: header → URL → current; Sunset header (RFC 8594) for deprecated; 410 warning for expired;
  `list_versions()` sorted by introduced_at
- `RequestDeduplicator(ttl_ms)` — `execute(idempotency_key, handler, request_hash)` → (response, was_duplicate);
  `purge_expired()` → count removed; `store_size()` → int; prevents double-charges, duplicate resource creation

Five demo scenarios: rate limiting (burst vs. exact), circuit breaker state machine, request coalescing
(5 concurrent threads → 1 backend call), API key lifecycle (create→validate→rotate→revoke),
API version routing (current / deprecated / expired), request deduplication (payment idempotency).

**`docs/implementation-note-12.md`** — "API Gateway Patterns: The Six Problems Every Production
Gateway Must Solve" — covers token bucket vs. sliding window tradeoffs, circuit breaker state machine
design, coalescing vs. caching distinction, API key lifecycle, version sunset policy, and idempotency
key TTL design.

**Tests:** `tests/test_api_gateway_patterns.py` — 61 tests covering all six patterns including
timing-sensitive tests (window sliding, TTL expiry, circuit breaker probe). Full suite: 359 passed.

---

## [0.12.0] — 2026-04-13

### Added — Distributed Cache Patterns

**`examples/13_distributed_cache_patterns.py`** — reference implementation of seven enterprise
distributed caching patterns addressing the five core failure modes that caching introduces:
stampede/thundering herd, write consistency, cross-node invalidation, TTL coordination, and cold-start.

New classes (self-contained in the example):
- `CacheEntry` — cache entry with value, created_at, expires_at, version, last_accessed_at;
  `is_expired` property; `ttl_remaining_ms` property
- `WriteBufferEntry` — pending write-behind entry with key, value, enqueued_at, attempts
- `LockState` — ACQUIRED, EXPIRED, RELEASED
- `FencingToken` — monotonically increasing lock grant: token_id, lock_key, holder_id, sequence,
  granted_at, expires_at, state; `is_valid` property for pre-write validation
- `InvalidationReason` — TTL_EXPIRED, EXPLICIT_DELETE, DEPENDENCY_CHANGED, CAPACITY_EVICTION,
  BUS_INVALIDATION
- `InvalidationEvent` — bus message: event_id, key, reason, source_node_id, timestamp, dependency_key
- `WarmupRecord` — priority-ordered warmup entry: key, value, priority, source
- `InMemoryStore` — thread-safe in-memory cache with TTL enforcement, hit/miss counters, `keys()`
  (excludes expired); simulates Redis/Memcached as cache backend
- `BackingDataStore` — simulated RDBMS with read/write counters and configurable latency
- `CacheAsidePattern` — lazy-load with stampede protection: acquire distributed lock before
  regenerating; double-check after lock acquisition; custom loader support; TTL jitter via
  TTLInvalidationStrategy
- `WriteThroughCache` — synchronous write to cache + store; store-fallback on cache miss;
  custom TTL per write; delete removes from both
- `WriteBehindCache` — immediate cache write + async buffer; `flush()` drains to store;
  dict-based deduplication (last writer wins); flow control via max_buffer_size auto-flush;
  flush_count and pending_count instrumentation
- `TTLInvalidationStrategy` — jitter (±factor% of base), sliding window renewal detection,
  dependency graph with transitive traversal (`register_dependency`, `get_dependent_keys`);
  cycle-safe via visited set
- `DistributedLockManager` — fencing token-based distributed lock; monotonic sequence counter
  per lock key; `acquire()` returns FencingToken or None; `release()` returns bool;
  `validate_token()` for pre-write fencing check; `is_locked()` for observability;
  thread-safe via threading.Lock
- `CacheInvalidationBus` — publish/subscribe invalidation fan-out; `subscribe()` registers
  node callbacks; `publish()` delivers to all subscribers; source node filtering in
  InvalidatingCache; `published_count()` and `last_event()` for observability
- `InvalidatingCache` — InMemoryStore + bus subscriber; `invalidate_and_publish()` combines
  local eviction + bus publication; `_eviction_count` for observability
- `CacheWarmupStrategy` — three warmup strategies: snapshot records (priority-sorted),
  key list replay (from access logs), store prefix scan; max_warmup_keys flow control;
  `loaded_count` accumulator

7 demo functions covering all patterns: cache-aside stampede protection, write-through,
write-behind with flush, TTL jitter and dependency graph, distributed lock with fencing tokens,
multi-node invalidation bus, and startup cache warmup.

**`docs/implementation-note-11.md`** — "Distributed Cache Patterns: When Adding a Cache Makes
Things Harder": explains the five cache failure modes (stampede, consistency, cross-node invalidation,
dependency invalidation, cold start), design rationale for each pattern, TTL jitter math,
fencing token split-brain prevention, production transport options (Redis Pub/Sub, Kafka,
Redis Streams), and a decision guide for pattern selection by use case.

Tests: 58 new tests in `tests/test_distributed_cache_patterns.py` — TestInMemoryStore (8),
TestCacheAsidePattern (6), TestWriteThroughCache (5), TestWriteBehindCache (6),
TestTTLInvalidationStrategy (7), TestDistributedLockManager (8), TestCacheInvalidationBus (5),
TestCacheWarmupStrategy (6), TestDemos (7)

---

## [0.11.0] — 2026-04-13

### Added — Event Streaming Temporal Window Patterns

**`examples/12_event_streaming_windows.py`** — reference implementation of temporal windowing
patterns for enterprise event streaming pipelines, covering the three canonical window types,
pluggable aggregation, watermark-based late-arrival handling, and a full StreamProcessor
orchestrator.

New classes (self-contained in the example):
- `StreamEvent` — frozen dataclass: event_id (UUID), stream_key (partition key), event_time (ms),
  ingestion_time (ms), payload (dict)
- `AggregationFunction` — COUNT, SUM, AVG, MIN, MAX, DISTINCT_COUNT
- `LateArrivalPolicy` — ACCEPT (re-open within tolerance), ASSIGN_TO_LATEST, DROP, SIDE_OUTPUT
- `WindowSpec` — window_size_ms, slide_interval_ms (SlidingWindow), session_gap_ms (SessionWindow),
  late_tolerance_ms, late_arrival_policy
- `WindowKey` — immutable (stream_key, window_start_ms, window_end_ms); hashable for dict keying
- `WindowResult` — finalized window: event_count, aggregations dict, late event counters, is_final
- `LateEventRecord` — audit record: policy_applied, watermark_at_arrival, lateness_ms, assigned_window
- `TumblingWindow` — non-overlapping fixed-duration windows; floor(event_time/W)*W assignment;
  each event belongs to exactly one window (no double-counting)
- `SlidingWindow` — overlapping windows advancing by slide_interval_ms; each event assigned to
  ceil(W/S) windows; burst events appear in multiple windows for smoothed rate metrics
- `SessionWindow` — activity-gap-based dynamic windows; gap >= session_gap_ms closes session and
  opens a new one; sessions are per stream_key and data-driven (not time-driven);
  `process_event()` returns (completed_key_or_None, current_key) for StreamProcessor integration
- `WindowAggregator` — single-pass aggregation over window events; (AggregationFunction, field_name)
  pairs are serializable (no lambda functions)
- `LateArrivalHandler` — enforces late-arrival policy; SIDE_OUTPUT accumulates events in
  reconciliation stream; all policies produce audit log entries
- `StreamProcessor` — orchestrates window assignment, per-window event accumulation, watermark
  advancement (`max(event_time) - allowed_lateness_ms`), and window finalization on watermark
  advance; session windows use `_session_accumulators` dict (separate from `_window_events`)
  that is flushed to `_window_events` when each session closes

Four scenarios: tumbling order pipeline (3×20-event windows, no double-counting), sliding API
rate monitoring (burst appears in multiple windows), session user journey (browse session +
purchase session), late-arrival handling (DROP / SIDE_OUTPUT / within-tolerance ACCEPT).

**`docs/implementation-note-10.md`** — "Temporal Windowing Patterns for Enterprise Event Streams"
covering tumbling vs. sliding vs. session semantics, watermark model and late-arrival policies,
aggregation design for serializability, exactly-once considerations, and distributed state
management mapping to Apache Flink/Spark Structured Streaming patterns.

**Test coverage:** 41 tests (`tests/test_event_streaming_windows.py`)

---

## [0.10.0] — 2026-04-13

### Added — Process Manager Pattern for Distributed Transaction Coordination

**`examples/11_process_manager.py`** — Process Manager pattern for long-running
distributed business transactions with explicit state machine, LIFO compensating
transaction registry, durable crash-recovery checkpointing, and per-step SLA timeouts.

New classes (self-contained in the example):
- `ProcessState` — lifecycle states: PENDING, RUNNING, COMPENSATING, COMPLETED,
  COMPENSATED, FAILED
- `StepOutcome` — step result: SUCCESS, FAILURE, TIMEOUT, SKIPPED
- `ProcessStep` — forward action + compensation + SLA timeout + idempotency key
  generator; compensation must be idempotent
- `CompletedStepRecord` — snapshot of a completed forward step pushed to the LIFO
  registry: step reference, action result, completed_at, idempotency key
- `CompensatingTransactionRegistry` — LIFO stack of completed steps; `compensate_all()`
  pops and executes compensations in reverse order; returns False if any compensation
  fails (process enters FAILED state requiring manual intervention)
- `ProcessCheckpoint` — serializable snapshot: process_id, next_step_index,
  completed_step_ids, registry_snapshot (JSON-serializable); `serialize()` / `from_dict()`
  for durable storage; written before each step execution for crash recovery
- `ProcessManager` — orchestrates step execution, checkpoint writes, SLA detection,
  LIFO compensation, and crash recovery via `execute()` and `resume_from_checkpoint()`
- `ProcessManagerAuditRecord` — SOX/FAR-compliant audit: complete forward step log,
  compensation log, timeout_reason, failure_reason, final context

Key design decisions:
- **LIFO is a correctness requirement, not a convention:** undoing steps in insertion
  order would corrupt dependent states (e.g. void payment before unreserving inventory)
- **Checkpoint written before each step:** ensures crash recovery resumes at the
  correct boundary; combined with idempotency keys, re-execution is safe
- **SLA timeout = same path as step failure:** both trigger LIFO compensation;
  the action may have committed side effects before timeout detection — document this
  in your runbook and consider a DeadLetterRouter for late-completing steps
- **Failed compensation → FAILED state:** manual intervention is required when
  a compensation step itself fails; the `failed_compensation_steps` counter in the
  audit record surfaces this for operational alerting
- **Injectable clock (`now_fn`):** enables deterministic SLA timeout tests without
  real sleeps

Four scenarios:
- A: Happy path — all four steps succeed (COMPLETED)
- B: Payment failure at step 2 — LIFO compensates inventory reservation (COMPENSATED)
- C: Shipment SLA timeout — LIFO compensates payment + inventory (COMPENSATED)
- D: Crash recovery — rehydrate from checkpoint at step 2, resume and complete

Tests: 28 new test cases in `tests/test_process_manager.py`.

**`docs/implementation-note-09.md`** — "Process Manager Pattern for Distributed
Transaction Coordination": state machine design, LIFO ordering rationale, SLA timeout
detection, checkpoint design, at-most-once semantics, SOX/FAR audit requirements,
relationship to Saga / ApprovalWorkflow / BackpressureController / SchemaRegistry.

---

## [0.9.0] — 2026-04-13

### Added — Schema Evolution and Consumer Version Compatibility Example + Implementation Note

**`examples/10_schema_evolution.py`** — schema evolution patterns for distributed
event-driven systems (Salesforce OpportunityUpdated pipeline, v1.0 → v2.0 migration):

New classes (self-contained in the example):
- `FieldEvolutionRule` — classification of field changes by compatibility impact:
  NEW_OPTIONAL_FIELD + REQUIRED_TO_OPTIONAL + FIELD_DEPRECATED (fully compatible);
  FIELD_REMOVED (forward compatible); FIELD_RENAMED + TYPE_CHANGED + NEW_REQUIRED_FIELD
  (breaking — require major version bump)
- `SchemaVersion` — semantic version (major.minor.patch) with explicit compatibility
  contracts: `is_backward_compatible_with()` and `is_forward_compatible_with()`
  enforce the same-major-version rule; cross-major requires migration
- `FieldSchema` — per-field definition: name, type, required, default, deprecated_since
- `EventSchema` — versioned schema with `validate()` (required fields check) and
  `apply_defaults()` (fills optional field defaults for cross-version reads)
- `SchemaRegistry` — central schema store: `register()`, `validate()`,
  `resolve_reader(producer_version, consumer_version)` → compatibility mode + notes
- `MigrationRule` — per-field transformation: FIELD_RENAMED (rename key),
  TYPE_CHANGED (apply transform callable), NEW_REQUIRED_FIELD (inject default)
- `SchemaMigration` — versioned migration spec: `apply(event)` transforms payload
- `EventMigrator` — BFS migration chain discovery; `migrate(event, from, to)` applies
  multi-step migration (v1.0 → v1.1 → v1.2 → v2.0); `can_migrate()` safety check
- `ConsumerVersionGroup` — consumers grouped by schema version for fanout
- `MultiVersionFanout` — delivers events to N consumer version groups using the correct
  transformation per group: exact_match / forward_compatible / backward_compatible /
  migrated; unresolvable paths surface `migration_failed` (not silent null delivery)

The OpportunityUpdated schema evolution sequence:
- v1.0 → v1.1: NEW_OPTIONAL_FIELD (`opportunity_score`, default=0.0) — backward+forward
- v1.1 → v1.2: NEW_OPTIONAL_FIELD (`region`, default="UNKNOWN") — backward+forward
- v1.2 → v2.0: FIELD_RENAMED (`amount`→`amount_usd`, `owner_id`→`rep_id`) — breaking

Scenarios:
- A: Backward compatible — v1.0 consumer reads v1.1 event; `opportunity_score` ignored
  (v1.0 schema has no such field; no error)
- B: Forward compatible — v1.1 consumer reads v1.0 event; `apply_defaults()` fills
  `opportunity_score=0.0`
- C: Breaking change — v1.0 event migrated to v2.0 via 3-step chain; `amount_usd` and
  `rep_id` correctly renamed; v2.0 schema validation passes; `amount` and `owner_id`
  not present in migrated payload
- D: Multi-version fanout — producer at v1.2 delivers to consumers at v1.0 (exact_match
  on payload, region stripped), v1.1 (backward_compatible), v1.2 (exact_match), v2.0
  (migrated: amount→amount_usd, owner_id→rep_id)

**`docs/implementation-note-08.md`** — "Schema Evolution in Enterprise Integration:
How to Change Event Schemas Without Breaking Consumers":
- Silent data corruption anatomy: how `amount`→`amount_usd` rename corrupts finance
  records without any error or alert
- The four evolution rules: fully compatible / forward-compatible-only / breaking
- Major/minor version contract enforcement
- Migration chain design: step-by-step vs. leap-of-faith
- Multi-version fanout operational pattern: version group registry lifecycle
- Schema validation as a required gate (not optional)
- What to do when migration path does not exist

Closes #30.

---

## [0.8.0] — 2026-04-13

### Added — Backpressure and Retry Storm Prevention Example + Implementation Note

**`examples/09_backpressure_retry.py`** — four patterns for keeping event-driven
integration pipelines stable under downstream system pressure:

New classes (self-contained in the example):
- `ErrorType` — classification for retry policy: TRANSIENT (retry with backoff),
  RESOURCE_EXHAUSTED (retry with longer base), NON_RETRYABLE (dead-letter immediately,
  no budget consumed), UNKNOWN (treat as TRANSIENT)
- `AdaptiveRetryStrategy` — exponential backoff with full jitter (AWS Architecture Blog
  formula: `random_between(0, min(cap, base * 2^attempt))`); per-error-type policy;
  RESOURCE_EXHAUSTED uses a 4× longer base interval
- `RetryBudget` — shared retry budget across the consumer in a rolling time window;
  when exhausted, events route to dead-letter immediately rather than blocking
  indefinitely; NON_RETRYABLE errors do not consume budget
- `DownstreamHealth` — observable metrics (latency_ms, error_rate, queue_depth) with
  composite `pressure_score` (latency 50% + error_rate 30% + queue_depth 20%)
- `BackpressureController` — watermark-based consumer state machine with hysteresis
  gap; high_watermark → THROTTLED; critical_watermark → PAUSED; falls back to NORMAL
  only when pressure drops below low_watermark (prevents oscillation)
- `DeadLetterRecord` — preserves original event + full retry history + final error
  + per-error-type TTL (TRANSIENT=24h, RESOURCE_EXHAUSTED=1h, NON_RETRYABLE=indefinite)
- `DeadLetterRouter` — triage buffer with `replay()` (re-inject) and `discard()` (audited)
- `IntegrationPipelineProcessor` — combines all four patterns; evaluates backpressure
  before each attempt; classifies errors before consuming retry budget

Scenarios:
- A: Normal pipeline — 3 events succeed without retries; dead-letter empty
- B: Oracle ERP slowdown — BackpressureController signals THROTTLED; first attempt fails
  (TRANSIENT); retry with jittered backoff succeeds; budget consumed=1
- C: Permanent schema failure — NON_RETRYABLE classified; dead-lettered immediately;
  retry budget unchanged (before=20, after=20); TTL=None (indefinite, human triage)
- D: Retry storm demonstration — no-jitter shows all 5 consumers retry at t+2.00s
  (synchronized convoy); full jitter spreads retries across [0, 16s] window;
  tight budget (max=3) exhausted by first event; remaining 4 events dead-lettered
  immediately without consuming budget

**`docs/implementation-note-07.md`** — "Backpressure and Retry Storm Prevention:
Why Most Integration Pipelines Fail Under Load":
- The failure sequence from slow downstream to full retry storm
- Error classification: why NON_RETRYABLE must bypass retry budget entirely
- Full jitter formula and convoy prevention
- Retry budget design: window sizing, budget sizing, relationship to saga timeouts
- Backpressure watermark design: hysteresis gap, composite pressure score
- Dead-letter queue: TTL by error type, replay vs. discard
- What this pattern does not cover: distributed rate limiting, circuit breaker
  persistence, priority queues, observability integration

Closes #29.

---

## [0.7.0] — 2026-04-13

### Added — Approval Workflow Example and Implementation Note

**`examples/08_approval_workflow.py`** — multi-step approval workflow with SLA enforcement,
escalation, and N-of-M quorum approval for enterprise procurement and contract management:

New classes (self-contained in the example):
- `ApprovalDecision` — immutable record of a single approver's decision: `approver_id`,
  `outcome` (APPROVED/REJECTED/TIMED_OUT), `reason`, `decided_at`, `sla_deadline`, `sla_met`
- `ApprovalGate` — single-approver gate with configurable `sla_hours` and `escalation_fn`;
  `request_approval(context, decision_fn, now=None)` accepts an injectable clock for
  deterministic SLA testing; calls `escalation_fn` immediately on timeout
- `QuorumApprovalGate` — N-of-M approval gate; short-circuits to REJECTED when enough
  rejections make quorum mathematically impossible; returns `(final_outcome, [ApprovalDecision])`
  covering all approvers who responded within the SLA
- `ApprovalWorkflow` — sequential saga orchestrator; adds gates via `add_step()` (fluent API);
  `execute()` stops on first non-APPROVED outcome; returns a complete `ApprovalAuditRecord`
- `ApprovalAuditRecord` — SOX/FAR-compliant audit record: `workflow_id`, `workflow_name`,
  `context` snapshot, ordered `gate_decisions`, `final_outcome`, `total_sla_met`,
  `initiated_at`, `completed_at`
- `ApprovalOutcome` — APPROVED, REJECTED, TIMED_OUT

Scenarios:
- A: Happy path — 3-step sequential workflow (department_head → legal_review →
  finance_approval); all approved within SLA; full audit record emitted
- B: Step 2 rejection — legal_review rejects; workflow terminates immediately; only
  2 gate decisions in audit record; finance step never executed
- C: SLA timeout with escalation — manager_approval times out (clock injected past
  48-hour deadline); `escalation_fn` fires immediately; timeout recorded as a decision
  in the audit trail
- D: 2-of-3 VP quorum — `QuorumApprovalGate` with quorum=2; VP-1 approves, VP-2 approves,
  VP-3 abstains; quorum met; all three decision records captured; workflow proceeds

**`docs/implementation-note-06.md`** — "Approval Workflows in Enterprise Integration:
When Human Decision Is a First-Class Event":
- Structural failure modes: stateless approval, timeout-as-afterthought, implicit quorum,
  flat audit trail — and the design fix for each
- Approval gate as a state machine: PENDING → APPROVED/REJECTED/TIMED_OUT; why escalation
  must be co-located with the gate definition, not a cron job
- Quorum short-circuit rule: early REJECTED when `(total - rejections) < quorum`
- Sequential vs. quorum: decision matrix for when to use each
- Integration with the saga pattern and transactional outbox: where approval fits in the
  compensatable transaction chain
- SOX Section 404 and FAR Subpart 15.4 requirements mapped to `ApprovalAuditRecord` fields
- Deterministic-time injection for SLA testing (the `now=` parameter pattern)
- What the pattern does not cover: persistent state across restarts, approval channel UI,
  concurrent decisions from the same approver

Closes #28.

---

## [0.6.0] — 2026-04-13

### Added — CQRS + Event Sourcing Example and Implementation Note

**`examples/07_cqrs_event_sourcing.py`** — CQRS (Command Query Responsibility Segregation)
and event sourcing applied to a Salesforce CRM opportunity pipeline:

New classes (all self-contained in the example, no core dependency changes):
- `EventStore` — append-only event log with optimistic concurrency (`expected_version`),
  temporal replay (`get_events_before(aggregate_id, cutoff)`), and subscription-based
  saga triggers (`subscribe(event_type, handler)`)
- `CommandBus` — routes write commands to handlers; each handler loads aggregate state
  by replaying events, validates preconditions, and appends new events
- `OpportunityProjection` — denormalized read model maintained by subscribing to domain
  events; supports `rebuild(all_events)` for zero-data-loss projection reconstruction
- `QueryBus` — routes read queries to projections; has no reference to `EventStore`,
  enforcing the CQRS separation structurally
- `ConcurrencyConflictError` — raised when `expected_version` does not match current
  stream version (optimistic locking violation)

Scenarios:
- A: Full 5-step lifecycle — `CreateOpportunity` → `UpdateStage` × 3 → `CloseWon`;
  read model reflects current state after every event
- B: Temporal query — reconstruct opportunity state at any past timestamp by replaying
  only events that occurred before the cutoff
- C: Optimistic concurrency conflict — second concurrent update with stale
  `expected_version` raises `ConcurrencyConflictError`
- D: Read model rebuild — drop and replay all events; rebuilt projection is byte-for-byte
  identical to the original (zero data loss demonstrated)
- E: Cross-aggregate saga — `CloseWon` event triggers a commission calculation saga
  via `EventStore.subscribe()` without any polling

**`docs/implementation-note-05.md`** — "CQRS in Enterprise Integration: When to Separate Reads from Writes":
- Decision criterion: when write shape and query shape diverge enough to justify CQRS
- Event sourcing: state as a function of events; temporal queries and causal replay
- CommandBus / QueryBus structural separation — why code-level enforcement beats convention
- Optimistic concurrency: the `expected_version` contract and retry pattern
- Read model rebuild: operational patterns enabled (schema migration, bug fix, new projections)
- When NOT to use CQRS + event sourcing (configuration data, reference data, simple CRUD)
- Integration with the transactional outbox pattern

Closes #27.

---

## [0.5.4] — 2026-04-13

### Added — End-to-End Integration Example

**`examples/06_end_to_end_integration.py`** — all four core patterns working together in a
Salesforce Contact Created → SAP S/4HANA Customer Master Record synchronization flow:
- Step 1: `WebhookHandler` — HMAC-SHA256 signature verification of inbound Salesforce webhook
- Step 2: `ProcessedWebhookStore` — idempotent reception; duplicate event_ids rejected
- Step 3: `CDCEvent.from_debezium()` — webhook payload normalized to canonical CDC envelope
- Step 4: `SagaOrchestrator` — 4-step saga (validate → SAP create → CRM sync → welcome email)
  with automatic compensation on failure (reverse order rollback)
- Step 5: `OutboxProcessor` — at-least-once broker relay for all saga completion events
- Scenario A: happy path — all 4 steps succeed; 3 events published to broker
- Scenario B: compensation — CRM sync fails after SAP succeeds → `SAPCustomerDeleted` +
  `SagaCompensated` published; SAP record cleanly rolled back
- Scenario C: webhook replay — duplicate `event_id` identified by deduplication store, skipped
- Integration pattern map showing component × responsibility breakdown
- Closes #22.

---

## [0.5.3] — 2026-04-13

### Added — Observability, Recovery Runbook, and Orchestration Boundary ADR

**`docs/implementation-note-04.md`** — "Observability and Recovery for Enterprise Integration":
- Event pipeline metrics: `events.dispatched/processed/failed/skipped/dead_lettered`, consumer lag
- Failure classification: Class 1 (transient), Class 2 (permanent), Class 3 (poison)
- Step-by-step recovery runbook: triage DLQ, fix root cause, safe Class 1 replay
- Deduplication observability: `dedup.store_hits/misses/errors`
- Structured event logging pattern using `to_audit_line()`
- Real incident case study showing the cost of missing observability
- Closes #3.

**`docs/adr/003-orchestration-vs-integration-boundaries.md`** — "Workflow Orchestration
Boundaries: What belongs in orchestration vs integration":
- Hard boundary: integration owns transport (retry, dedup, broker format); orchestration
  owns business process (step sequence, approval checkpoints, compensation, escalation)
- Test for boundary: "if I replace the broker, does this code change?"
- What does NOT belong in the integration layer (business approval logic, SLA tracking)
- Domain event interface between layers: `CRMUpdateApplied` vs `ProcessApproved`
- Rejected alternatives: stateful integration handlers, orchestration-aware envelopes
- Closes #2.

---

## [0.5.2] — 2026-04-13

### Added — Idempotent CRM Update Example

**`examples/05_idempotent_crm_update.py`** — end-to-end idempotent event flow
for a Salesforce Lead Converted → SAP S/4HANA Contact Sync:
- `ProcessedEventStore` — simulates Redis SETNX / DB unique constraint deduplication
- Scenario A: first delivery, 2 transient failures, succeeds on attempt 3
- Scenario B: broker redelivers same event_id → SKIPPED (update not re-applied)
- Scenario C: new event with different event_id → applied on first attempt
- Scenario D: persistent failure → dead-lettered after retry budget exhausted
- Audit trail: `CRMUpdateResult` per event with outcome + attempt count
- Five structural idempotency rules with rationale
- Closes #1.

---

## [0.5.1] — 2026-04-13

### Added — CRM-ERP Sync Boundary Example and Implementation Note

**`examples/04_crm_erp_sync_boundary.py`** — Runnable end-to-end demo of the `SyncBoundary`
pattern applied to a Salesforce ↔ SAP S/4HANA account synchronization scenario:
- Field authority map (SF owns segment/email, SAP owns billing/credit limit, legal name is shared)
- Change detection distinguishing clean updates (one system changed) from true conflicts (both changed)
- Three resolution strategies: authority-wins, last-writer-wins, manual-review
- Shared fields (`legal_entity_name`, `vat_registration_number`) always escalate to manual review
- `allow_system_b_override=True` surfacing unexpected writes from non-authoritative systems
- Excluded fields (`sf_lead_score`, `sap_cost_center`) never touched by sync engine
- Closes #5.

**`docs/implementation-note-03.md`** — "Sync Boundaries in CRM-ERP Integration":
why explicit field authority prevents data corruption in bi-directional sync.
Covers the silent-overwrite failure mode, update-loop failure mode, last-sync-value
conflict detection (vs naive timestamp comparison), and when each resolution
strategy is appropriate.

---

## [0.5.0] — 2026-04-13

### Added — Multi-Broker Envelope Adapters

**AWS SQS/SNS** (`sqs_envelope.py` — `SQSEventEnvelope`):
- Standard queues and FIFO queues (`message_group_id` / `message_deduplication_id`).
- `to_send_params()`: produces a dict directly passable to `boto3` `sqs.send_message(**params)`.
- `from_sqs_message()`: deserialises from SQS message dict; transparently unwraps SNS fan-out
  notifications (``{"Type": "Notification", ...}`` wrapper).
- `mark_dead_letter(reason)`, `is_fifo()`, `approximate_receive_count` tracking.
- Closes #8.

**Azure Service Bus** (`azure_servicebus_envelope.py` — `AzureServiceBusEnvelope`):
- Sessions (ordered, grouped delivery per `session_id` — analogous to Kafka partition keys).
- `to_service_bus_message()`: produces a dict for `azure-servicebus` `ServiceBusMessage(**params)`.
- `from_service_bus_message()`: accepts SDK object or test-friendly dict; handles multi-frame body.
- Scheduled delivery (`enqueue_time_utc`), correlation ID, `mark_dead_letter(reason, description)`.
- `is_session_enabled()`, `is_scheduled()`.

**GCP Pub/Sub** (`gcp_pubsub_envelope.py` — `GCPPubSubEnvelope`):
- Ordering keys for per-key sequential delivery.
- `to_publish_data()` / `to_publish_attributes()`: separate data bytes + string attributes for
  `publisher.publish(topic, data=..., **attrs)`.
- `from_pubsub_message()`: pull subscription (SDK object or dict).
- `from_push_payload()`: push subscription HTTP body (base64 decode + SNS-style unwrap).
- `delivery_attempt` tracking, `exceeds_delivery_attempts(n)` for manual DLQ routing.
- Partially closes #4.

**RabbitMQ AMQP** (`rabbitmq_envelope.py` — `RabbitMQEnvelope`):
- Exchange / routing key / topic exchange routing.
- `to_amqp_body()` / `to_amqp_properties()`: body bytes + pika `BasicProperties`-compatible dict.
- `from_amqp_message()`: accepts pika `BasicProperties` or dict; parses `x-death` header for
  DLX death count tracking.
- Per-message TTL (`expiration`), priority, request/reply (`reply_to`, `correlation_id`).
- `mark_dead_letter(reason)`, `is_dead_lettered()`, `redelivered` flag.
- Closes #6.

### Changed
- `kafka_envelope.py`: `to_producer_record()` now uses `ensure_ascii=False` so non-ASCII
  characters (e.g. names with diacritics) are preserved verbatim in Kafka message bytes.
- `__init__.py`: now exports all modules — `KafkaEventEnvelope`, `SQSEventEnvelope`,
  `AzureServiceBusEnvelope`, `GCPPubSubEnvelope`, `RabbitMQEnvelope`, `CDCEvent`,
  `CDCOperation`, `CDCSourceMetadata`, `CircuitBreaker`, `CircuitOpenError`, `CircuitState`,
  `SagaOrchestrator`, `SagaStep`, `SagaResult`.

### Tests
- 65 new tests in `tests/test_broker_envelopes.py` (create, serialise, deserialise, roundtrip,
  DLQ, SNS-unwrap, push-payload, x-death count for all four adapters).
- Total: **171 passing**.

---

## [0.4.1] — 2026-04-13

### Added
- `rate_limiter.py`: `SlidingWindowRateLimiter` — thread-safe sliding-window rate limiter for enterprise CRM/ERP API rate windows (Salesforce 24h, HubSpot 10s, etc.). Tracks request timestamps in a rolling deque; enforces hard count limits within configurable windows. Supports blocking/non-blocking `acquire()`, `async_acquire()`, `try_acquire()`, `utilization()` (returns 0.0–1.0 utilization fraction), and `requests_in_window()`. Closes #21.
- `__init__.py`: exports `SlidingWindowRateLimiter`

---

## [0.4.0] — 2026-04-12

### Added
- `rate_limiter.py`: `TokenBucketRateLimiter` — thread-safe token-bucket rate limiter for webhook delivery, API call bursting, and event-driven integration rate management. Supports both blocking and non-blocking `acquire()`, plus async `async_acquire()` for FastAPI/asyncio environments. `RateLimitExceeded` exception with `tokens_available` and `cost` attributes.
- `[pydantic]` optional dependency: `pydantic>=2.12.0` for Pydantic v2 model validation in webhook payloads
- `[all]` extra combining kafka, fastapi, and pydantic dependencies

### Changed
- Bumped ecosystem compatibility pins:
  - `fastapi`: `>=0.100.0` → `>=0.130.0` (FastAPI 0.135.3 current; Server-Sent Events + streaming JSON Lines support)
  - `httpx`: `>=0.24.0` → `>=0.27.0`
- `pyproject.toml`: version bumped to 0.4.0

---

## [Unreleased]

### Added
- `http/fastapi_router.py`: `WebhookRouter` — FastAPI `APIRouter` factory for HMAC-verified, idempotency-safe webhook endpoints. Supports async and sync handler callables. Install with `integration-automation-patterns[fastapi]`
- `outbox.py`: `AsyncOutboxProcessor` — async variant that accepts coroutine or sync callables for all I/O operations; for FastAPI, aiohttp, and asyncio-based applications

---

## [0.3.0] — 2026-04-12

### Added
- Enhanced CI: coverage reporting (Codecov), ruff format check, build-check job, pip cache, concurrency cancellation
- Automation: PR auto-labeler, stale bot, Conventional Commits PR title check, first-contributor welcome bot
- Dependabot weekly pip + Actions updates; CODEOWNERS; SECURITY.md; pre-commit config
- `http/webhook_handler.py`: `IdempotentWebhookReceiver` — HMAC validation, idempotency key extraction, bounded LRU duplicate cache
- ADRs: `docs/adr/001-idempotency-key-as-event-id.md`, `002-explicit-field-authority-model.md`, `003-saga-choreography-vs-orchestration.md`
- README: badges, ASCII pipeline diagram, pattern catalog table, quickstart, BibTeX citation
- GitHub Discussions enabled; 22 standardized labels; milestones v0.3.0 + v1.0.0

---

## [0.2.0] — 2026-04-11

### Added

**Resilience patterns:**
- `circuit_breaker.py` — thread-safe circuit breaker with CLOSED/OPEN/HALF_OPEN state machine:
  - `CircuitBreaker` — configurable failure threshold, recovery timeout, half-open probe
  - `CircuitState` — CLOSED, OPEN, HALF_OPEN with automatic transitions
  - `CircuitBreakerStats` — snapshot of failure count, state, and last failure time
  - `CircuitOpenError` — raised when calls are rejected in OPEN state
  - `.call` decorator for transparent wrapping
- `saga.py` — distributed saga orchestrator for multi-step workflows:
  - `SagaOrchestrator` — forward execution + automatic backward compensation on failure
  - `SagaStep` — named step with `action` and `compensate` callables
  - `SagaResult` — succeeded flag, failed_step name, compensated_steps list, per-step results
  - Fluent `.add_step()` chaining API

**Messaging patterns:**
- `outbox.py` — transactional outbox for at-least-once event delivery:
  - `OutboxRecord` — pending outbox entry with UUID, aggregate_id, event_type, payload
  - `OutboxProcessor` — broker-agnostic relay: fetch_pending → publish → mark_published/mark_failed
  - `process_batch()` returns (published_count, failed_count) tuple
- `kafka_envelope.py` — Kafka-aware event envelope:
  - `KafkaEventEnvelope` — partition key, topic, schema version, DLQ routing
  - `.to_producer_record()` — serialised to `{topic, key, value: bytes, headers}`
  - `.from_consumer_record()` — deserialise from consumed record
  - `.mark_dead_letter(reason)` — immutable copy routed to `<topic>.dlq`
- `webhook_handler.py` — HMAC-SHA256 signed webhook verification:
  - `WebhookHandler` — validates `sha256=<hex>`, `v1=<hex>`, and `t=<ts>,v1=<hex>` formats
  - `WebhookEvent` — typed, verified event with event_id, event_type, payload, raw bytes
  - `WebhookSignatureError` / `WebhookReplayError` — specific error classes

**Database change capture:**
- `cdc_event.py` — Change Data Capture event envelope:
  - `CDCEvent` — typed CDC record with operation, source metadata, before/after snapshots
  - `CDCOperation` — INSERT, UPDATE, DELETE, SNAPSHOT, TRUNCATE
  - `CDCSourceMetadata` — database, schema, table, connector, LSN, server_id
  - `.from_debezium()` — parse standard Debezium envelope format
  - `.changed_fields()` — set of field names that differ between before/after
  - `.to_audit_dict()` — JSON-serialisable audit record

**OSS infrastructure:**
- `CODE_OF_CONDUCT.md` — Contributor Covenant 2.1
- `ECOSYSTEM.md` — integration map across message brokers, CDC connectors, and frameworks
- Issue templates: new-pattern, new-framework-integration

### Changed
- `pyproject.toml` → version `0.2.0`; added `kafka` optional dependency group; expanded keywords

---

## [0.1.0] — 2026-04-11

### Added

**Core modules:**
- `event_envelope.py` — reliable event transport for enterprise integration:
  - `EventEnvelope` — idempotency key, delivery status lifecycle, structured audit logging
  - `DeliveryStatus` — PENDING → DISPATCHED → ACKNOWLEDGED → PROCESSED / RETRYING / FAILED / SKIPPED
  - `RetryPolicy` — bounded retry with fixed or exponential backoff and configurable cap
- `sync_boundary.py` — system-of-record synchronization contracts:
  - `SyncBoundary` — explicit field-level authority assignment between two systems
  - `RecordAuthority` — SYSTEM_A / SYSTEM_B / SHARED / MANUAL ownership model
  - `SyncConflict` — structured representation of detected bi-directional update conflicts

**Documentation:**
- `docs/architecture.md` — layered integration architecture (source → integration → workflow → SOR → observability)
- `docs/implementation-note-01.md` — event-driven integration reliability patterns

**Project infrastructure:**
- `LICENSE` — MIT
- `CITATION.cff` — enables GitHub "Cite this repository" button
- `CONTRIBUTING.md` — contribution guidance
- `GOVERNANCE.md` — project governance model
- `ROADMAP.md` — near-term development direction
- `pyproject.toml` — full build configuration with keywords, classifiers, and optional dependency groups
- GitHub Actions CI: pytest (Python 3.10–3.12), ruff lint, mypy type check
- Issue templates: bug report, feature request
- 42 passing tests covering all public module APIs
