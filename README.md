# integration-automation-patterns

[![CI](https://github.com/ashutoshrana/integration-automation-patterns/actions/workflows/ci.yml/badge.svg)](https://github.com/ashutoshrana/integration-automation-patterns/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/integration-automation-patterns.svg)](https://pypi.org/project/integration-automation-patterns/)
[![Python](https://img.shields.io/pypi/pyversions/integration-automation-patterns.svg)](https://pypi.org/project/integration-automation-patterns/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/integration-automation-patterns.svg)](https://pypi.org/project/integration-automation-patterns/)

**Reference patterns for reliable enterprise integration, workflow automation, and event-driven systems — 40 examples, 1780 tests.**

Structural solutions to the recurring failure modes of enterprise integration: duplicate event processing, partial transaction failures, silent data conflicts, and unrecoverable workflow state.

---

## The problem this solves

Enterprise systems that span CRM + ERP + messaging fail in predictable ways. This library provides reference implementations of the patterns that solve these problems structurally — idempotent event envelopes, field-level authority boundaries, saga compensation, transactional outbox, event sourcing with optimistic concurrency, and API gateway composition — so integration logic is explicit, testable, and broker-agnostic.

---

## Architecture overview

```
Inbound Event
     │
     ▼
EventEnvelope (event_id, idempotency_key, source, schema_version)
     │
     ├─ Duplicate Check ──────────► skip if already processed
     │
     ├─ Authority Check ──────────► SyncBoundary.detect_conflict()
     │                               which system owns this field?
     │
     ├─ Retry / Circuit Breaker ──► exponential backoff + OPEN/HALF_OPEN recovery
     │
     └─ Transactional Outbox ─────► write event + domain change in one DB tx
                                     relay delivers to broker independently
```

---

## Installation

```bash
pip install integration-automation-patterns
```

Optional extras:

```bash
pip install 'integration-automation-patterns[kafka]'
pip install 'integration-automation-patterns[fastapi]'
pip install 'integration-automation-patterns[pydantic]'
```

---

## Quick start

```python
from integration_automation_patterns.event_envelope import EventEnvelope
from integration_automation_patterns.sync_boundary import SyncBoundary, FieldAuthority

# Idempotent event — safe to process multiple times
event = EventEnvelope(
    event_id="evt_8f3a1b",
    source="salesforce",
    event_type="contact.updated",
    payload={"email": "alice@example.com", "phone": "+1-555-0100"},
    schema_version="1.0",
)

# Field-level authority: who owns each field?
boundary = SyncBoundary(
    authorities={
        "email": FieldAuthority(owner="salesforce", read_others=["erp"]),
        "phone": FieldAuthority(owner="erp",        read_others=["salesforce"]),
    }
)
conflicts = boundary.detect_conflict(
    incoming_system="salesforce",
    fields={"email": "alice@example.com", "phone": "+1-555-0100"},
)
# conflicts → {"phone": ConflictDetail(owner="erp", incoming_system="salesforce")}
```

---

## Example catalog — 40 patterns

| # | File | Pattern | Problem Solved |
|---|------|---------|---------------|
| 01 | `01_event_envelope_retry.py` | Idempotent Event | Duplicate processing under at-least-once delivery |
| 02 | `02_transactional_outbox.py` | Transactional Outbox | Reliable event publish in same DB transaction |
| 03 | `03_saga_compensation.py` | Saga | Multi-step distributed transaction with rollback |
| 04 | `04_crm_erp_sync_boundary.py` | Authority Model | Field-level conflict detection across systems |
| 05 | `05_idempotent_crm_update.py` | Idempotent CRM | Deduplication on CRM write operations |
| 06 | `06_end_to_end_integration.py` | End-to-End | Full CRM → ERP integration with all patterns combined |
| 07 | `07_cqrs_event_sourcing.py` | CQRS Basics | Command/query separation with projection |
| 08 | `08_approval_workflow.py` | Approval Workflow | Multi-stage approval with escalation |
| 09 | `09_backpressure_retry.py` | Backpressure | Rate-limited retry with bounded queue |
| 10 | `10_schema_evolution.py` | Schema Evolution | Forward/backward-compatible event schema versioning |
| 11 | `11_process_manager.py` | Process Manager | Long-running process coordination across services |
| 12 | `12_event_streaming_windows.py` | Streaming Windows | Tumbling/sliding window aggregation over event streams |
| 13 | `13_distributed_cache_patterns.py` | Cache Aside | Read-through, write-through, cache invalidation |
| 14 | `14_api_gateway_patterns.py` | API Gateway (v1) | Request routing, header injection, version dispatch |
| 15 | `15_saga_orchestration_patterns.py` | Saga Orchestration | Orchestrator-driven saga with explicit compensation |
| 16 | `16_event_sourcing_cqrs.py` | Event Sourcing (v1) | Append-only event log with basic projection |
| 17 | `17_resilience_patterns.py` | Resilience | Circuit breaker, bulkhead, timeout, retry hierarchy |
| 18 | `18_message_routing_patterns.py` | Message Routing | Content-based router + splitter + aggregator + filter |
| 19 | `19_workflow_orchestration_patterns.py` | Workflow Orchestration | 7-state machine (PENDING→COMPLETED/COMPENSATED) + saga compensation |
| 20 | `20_data_pipeline_patterns.py` | Data Pipeline / ETL | CheckpointStore (fault-tolerant resumability) + DataQualityValidator (5 rule types) + ETLPipeline |
| 21 | `21_api_gateway_patterns.py` | API Gateway (v2) | RateLimiter (token-bucket) + RequestTransformer + ResponseTransformer + APIVersionRouter + APIComposer (sequential + parallel) |
| 22 | `22_event_sourcing_cqrs_patterns.py` | Event Sourcing + CQRS (v2) | AggregateRoot + EventStore (optimistic concurrency) + SnapshotStore + Projection + EventSourcedRepository |
| 23 | `23_service_mesh_resilience.py` | Service Mesh Resilience | Bulkhead (concurrent slot control, BulkheadRejected) + TimeoutHierarchy (operation/service/request tiers) + HealthCheckAggregator (HEALTHY/DEGRADED/UNHEALTHY) + RetryWithJitter (exponential backoff + full jitter) |
| 24 | `24_distributed_tracing_patterns.py` | Distributed Tracing | W3C TraceContext (traceparent header) + Span (OTLP-compatible) + Tracer + AlwaysSample/NeverSample/RatioSampler + InMemorySpanExporter + TracerProvider + W3C Baggage |
| 25 | `25_schema_registry_patterns.py` | Schema Registry | InMemorySchemaRegistry (all 7 CompatibilityLevels) + AvroSchemaEvolution (backward/forward/full) + ConfluentFramingCodec (wire format) + JsonSchemaSerializer + SchemaEvolutionMigrator |
| 26 | `26_graphql_patterns.py` | GraphQL API | GraphQLSchema (SDL builder) + ResolverRegistry (decorator dispatch) + DataLoader (N+1 prevention, batch + prime) + CursorPagination (Relay spec) + FieldAuthorizationPolicy + SubscriptionManager |
| 27 | `27_event_driven_workflow_patterns.py` | Event-Driven Workflows | EventBus (pub/sub + wildcard) + EventSourcingWorkflow (append-only log + state replay) + ChoreographySaga (event-driven saga, no orchestrator) + DeadLetterQueue (retry + DLQ + recovery) + EventFilter (predicate chain + transformer chain) |
| 28 | `28_grpc_streaming_patterns.py` | gRPC Streaming | UnaryUnary (interceptor chain) + ServerStreaming (generator + limit) + ClientStreaming (list + generator drain) + BidirectionalStreaming (None-skip + queue) + StreamingInterceptor (named request/response transforms) |
| 29 | `29_async_concurrent_patterns.py` | Async/Concurrent | AsyncTaskPool (semaphore-bounded + ordered results) + AsyncRetry (exponential backoff + jitter + RetryExhausted) + AsyncPipeline (staged gather + None-drop) + ThreadSafeCache (RLock + TTL) + WorkerPool (ThreadPoolExecutor wrapper) |
| 30 | `30_message_broker_patterns.py` | Message Broker | MessageQueue (priority + FIFO, thread-safe) + TopicExchange (AMQP-style * and # routing) + FanoutExchange (broadcast to all subscribers) + MessageBatch (auto-flush at size) + DeduplicationFilter (time-window idempotency) |
| 31 | `31_service_discovery_patterns.py` | Service Discovery | ServiceRegistry (register/deregister/heartbeat/evict_stale, thread-safe) + LoadBalancer (round_robin/random/weighted strategies) + HealthChecker (configurable failure_rate simulation) + ServiceMesh (route/rebalance/register_and_check orchestration) |
| 32 | `32_observability_patterns.py` | Observability | MetricRegistry (thread-safe counters/gauges, snapshot, reset) + Histogram (configurable bucket boundaries, cumulative counts, percentile via linear interpolation) + SpanTracer (uuid-based trace/span recording, duration_ms, get_trace, completed_spans) + HealthCheck (check registry, run_all, is_healthy, summary) + AlertRule/AlertManager (threshold-based alerting, 5 condition types, fired_count accumulation) |
| 33 | `33_data_pipeline_patterns.py` | Data Pipelines | BatchProcessor (fixed-size batching + retry with per-batch retry loop + thread-safe stats) + StreamProcessor (tumbling/sliding windows, process_stream, aggregate) + ETLPipeline (fluent builder: extract/transform/load, run/run_dry/reset, timing) + DataLineage (directed graph, BFS lineage_path, upstream/downstream, to_dict) + DataQualityChecker (named rule registry, check_batch with violation counts, valid_only) |
| 34 | `34_distributed_transaction_patterns.py` | Distributed Transactions | TwoPhaseCommit (2PC coordinator: prepare/commit/rollback with participant vote) + CompensatingTransaction (Saga-style: LIFO compensation on failure, method-chaining add_step) + DistributedLock (token-based TTL lock, reentrant support, TTL extension, blocking acquire with timeout) + TransactionCoordinator (multi-resource coordinator: LIFO rollback, UUID-keyed enlisted resources) |
| 35 | `35_event_sourcing_advanced.py` | Event Sourcing Advanced | EventProjector (handler dispatch + state accumulation, reset) + EventUpcaster (version-ordered schema migrations, current_version) + EventReplay (since/by_aggregate/by_type/between filtering) + CausationChain (causation_id + correlation_id tracking, chain walk with cycle guard, correlation_group) + TemporalQuery (state_at point-in-time replay, events_in_window, latest_before) |
| 36 | `36_workflow_state_machine.py` | Workflow State Machine | StateMachine (FSM: add_state on_enter/on_exit + add_transition guard+action + trigger + can_trigger) + HierarchicalStateMachine (composite states: add_child_state + set_initial_child + enter_state auto-descend + is_in_state ancestor walk) + WorkflowEngine (multi-step orchestrator: per-step max_retries + exponential backoff + timeout_ms via ThreadPoolExecutor + WorkflowResult) + ConditionalRouter (first-match routing: register_route + register_default + route, ValueError on no match) + ParallelWorkflow (ThreadPoolExecutor: run with timeout + run_until_first_success cancels remaining) |
| 37 | `37_rate_limiting_patterns.py` | Rate Limiting / Throttling | SlidingWindowRateLimiter (deque-based per-client: allow/remaining/reset_at, thread-safe) + TokenBucketLimiter (burst capacity: lazy refill via monotonic(), consume(tokens)/available_tokens) + AdaptiveThrottler (AIMD: record_success/record_failure×0.5, allow() via internal token bucket) + QuotaManager (3-tier per-second/per-minute/per-day: atomic all-or-nothing consume, status, reset) + CircuitBreakerRateLimiter (sliding window + CLOSED/OPEN/HALF_OPEN: call(fn) raises RateLimitExceeded or CircuitOpen) |
| 38 | `38_reactive_streams_patterns.py` | Reactive Streams / Backpressure | BatchProcessor (configurable batch_size + flush_timeout_ms: thread-safe accumulate/flush, stats) + StreamProcessor (generator pipeline: transform/filter/window stage chaining, drain) + ETLPipeline (extract→transform→load with per-record error handling and ETLResult stats) + DataLineage (node/edge DAG: add_node/add_edge, get_upstream/get_downstream DFS, impact_analysis) + DataQualityChecker (completeness/uniqueness/range/pattern/custom rules, quality_score 0–1, DataQualityError) |
| 39 | `39_distributed_tracing_patterns.py` | Distributed Tracing / Observability | TraceContext (W3C traceparent: uuid4 trace_id/span_id, to_traceparent/from_traceparent, child_context(), sampling_flag) + Span (start/finish timing, add_event, set_status OK/ERROR/UNSET, to_dict() serialization, live duration_ms) + Tracer (active+completed span lists, threading.Lock, get_trace sorted by start_time, export) + SamplingStrategy (deterministic hash-based: always_on/always_off/ratio, should_sample) + TraceExporter (get_by_service/get_errors/get_slow_spans filters, clear) |
| 40 | `40_saga_choreography_patterns.py` | Saga Choreography / Distributed Transactions | SagaEvent (frozen dataclass: event_id/saga_id/event_type/payload/timestamp/sequence, create() classmethod UUID factory) + SagaParticipant (execute/compensate with event recording, executed_count/compensated_count) + SagaChoreographer (event bus: register_handler, start/complete/fail_saga, publish_event collects responses, get_saga_status/events) + CompensationEngine (reverse-order rollback: record_execution, compensate_all() reverses execution list, compensation_count) + SagaMonitor (configurable timeout_seconds, record_start/end, is_timed_out, get_stats 5-bucket, get_timed_out_sagas) |

---

## Pattern reference

### Core integration patterns

| Pattern | Class | Description |
|---------|-------|-------------|
| Idempotent Event | `EventEnvelope` | UUID event_id deduplication under at-least-once delivery |
| Authority Model | `SyncBoundary` | Field-level conflict detection — which system of record owns this field? |
| Circuit Breaker | `CircuitBreaker` | CLOSED → OPEN → HALF_OPEN state machine with configurable thresholds |
| Bulkhead | `Bulkhead` | Max concurrent + queue control; `BulkheadRejected` when both full |
| Timeout Hierarchy | `TimeoutHierarchy` | Three-tier: operation → service → request; `TimeoutExceeded(tier, budget_ms)` |
| Health Check Aggregator | `HealthCheckAggregator` | HEALTHY/DEGRADED/UNHEALTHY; `is_ready()` returns False only when UNHEALTHY |
| Retry with Jitter | `RetryWithJitter` | Exponential backoff + full jitter; `RetryExhausted(attempts, last_error)` |
| Saga | `SagaOrchestrator` | Multi-step distributed transaction with reverse-order compensation |
| Transactional Outbox | `OutboxPublisher` | Reliable publish in same DB transaction as domain change |
| CDC Event | `CDCEvent` | Typed INSERT/UPDATE/DELETE with Debezium-compatible parsing |
| Webhook Validation | `WebhookHandler` | HMAC-SHA256 signature verification + idempotency |

### Workflow and orchestration patterns

| Pattern | Class | Description |
|---------|-------|-------------|
| Workflow State Machine | `WorkflowStateMachine` | 7-state lifecycle: PENDING → RUNNING → COMPLETED / FAILED / COMPENSATING / COMPENSATED / CANCELLED |
| Compensation Engine | `CompensationEngine` | Reverse-order saga rollback with per-step compensating activities |
| Process Manager | `ProcessManager` | Long-running correlation across multiple services and events |
| Approval Workflow | `ApprovalWorkflow` | Multi-stage approval with timeout and escalation |

### Data and messaging patterns

| Pattern | Class | Description |
|---------|-------|-------------|
| Data Quality Validator | `DataQualityValidator` | COMPLETENESS / UNIQUENESS / RANGE / REGEX / CUSTOM rules with `QualityGateError` |
| Checkpoint Store | `CheckpointStore` | Stage + record-level resumability for fault-tolerant ETL |
| Content-Based Router | `ContentBasedRouter` | Route messages to handlers by payload predicate |
| Message Aggregator | `MessageAggregator` | Collect N messages into a batch with timeout |
| Streaming Windows | `TumblingWindow`, `SlidingWindow` | Event stream aggregation |

### API gateway patterns

| Pattern | Class | Description |
|---------|-------|-------------|
| Rate Limiter | `RateLimiter` | Token-bucket per client, thread-safe, raises `RateLimitExceeded(retry_after_seconds)` |
| Request Transformer | `RequestTransformer` | Header injection, body field rename, URL prefix rewrite |
| Response Transformer | `ResponseTransformer` | Status code remapping, response field masking (`"***"`) |
| Version Router | `APIVersionRouter` | Handler registry by version string; raises `UnsupportedVersion` |
| API Composer | `APIComposer` | Fan-out to multiple backends; sequential and parallel merge |

### Event sourcing and CQRS patterns

| Pattern | Class | Description |
|---------|-------|-------------|
| Aggregate Root | `AggregateRoot` | `on_<EventType>` handler dispatch, uncommitted event collection |
| Event Store | `EventStore` | Append-only with optimistic concurrency; `ConcurrencyConflict` on version mismatch |
| Snapshot Store | `SnapshotStore` | Aggregate state checkpoint to reduce event replay time |
| Projection | `AccountBalanceProjection` | Read model built from event stream; `rebuild()` and `reset()` |
| Repository | `EventSourcedRepository` | EventStore + SnapshotStore with auto-snapshot at configurable threshold |

---

## Repository structure

```
src/integration_automation_patterns/
├── event_envelope.py         # Reliable event transport + retry + delivery status
├── sync_boundary.py          # Bi-directional SOR sync with field authority
├── circuit_breaker.py        # CLOSED/OPEN/HALF_OPEN circuit breaker
├── saga.py                   # Distributed saga orchestrator + compensation
├── outbox.py                 # Transactional outbox for at-least-once delivery
├── kafka_envelope.py         # Kafka envelope: partition key, DLQ, schema version
├── webhook_handler.py        # HMAC-SHA256 webhook verification + idempotency
└── cdc_event.py              # CDC event types (Debezium-compatible)
examples/                     # 30 runnable pattern examples (see catalog above)
tests/                        # 977 passing tests
docs/
├── architecture.md
├── implementation-note-01.md # Event-driven integration reliability
├── implementation-note-02.md # Idempotency in enterprise event processing
└── adr/
    └── 001-idempotency-key-design.md
```

---

## Published notes

- [Implementation Note 01](./docs/implementation-note-01.md) — Event-driven integration reliability: why idempotency must be structural
- [Implementation Note 02](./docs/implementation-note-02.md) — Idempotency in enterprise event processing
- [ADR 001](./docs/adr/001-idempotency-key-design.md) — Idempotency key design: event_id as the deduplication unit

---

## Near-term roadmap

- GraphQL subscriptions with WebSocket transport
- Kafka + FastAPI integration examples with async support
- gRPC bidirectional streaming patterns

---

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md). Run `pytest tests/ -v` before opening a pull request.

---

## Citation

```bibtex
@software{rana2026iap,
  author  = {Rana, Ashutosh},
  title   = {integration-automation-patterns: Enterprise integration reliability patterns},
  year    = {2026},
  version = {0.33.0},
  url     = {https://github.com/ashutoshrana/integration-automation-patterns},
  license = {MIT}
}
```

---

## Part of the enterprise AI patterns trilogy

| Library | Focus | Coverage |
|---------|-------|---------|
| [enterprise-rag-patterns](https://github.com/ashutoshrana/enterprise-rag-patterns) | What to retrieve | 47 sectors · 64 regulations · 1722 tests |
| [regulated-ai-governance](https://github.com/ashutoshrana/regulated-ai-governance) | What agents may do | 38 governance examples · 25 jurisdictions · 2451 tests |
| **integration-automation-patterns** | How data flows | 40 patterns · schema registry · GraphQL · 1780 tests |

---

## License

MIT — see [LICENSE](LICENSE).
