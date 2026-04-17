# API Reference — integration-automation-patterns

All public symbols exported from `integration_automation_patterns`.

---

## Event envelope

### `EventEnvelope`

Idempotent event transport. Assigns a stable UUID-based `event_id` for deduplication.

```python
@dataclass
class EventEnvelope:
    event_id: str               # UUID — deduplication key
    source: str                 # originating system: "salesforce", "erp", etc.
    event_type: str             # domain event: "contact.updated"
    payload: dict               # event body
    schema_version: str         # for schema evolution
    retry_policy: RetryPolicy | None = None
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
    created_at: str = ""        # ISO 8601, auto-set
    correlation_id: str = ""    # tracing / saga correlation

    def mark_delivered(self) -> None: ...
    def mark_failed(self, error: str) -> None: ...
    def should_retry(self) -> bool: ...
    def to_dict(self) -> dict: ...
```

### `RetryPolicy`

```python
@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 60.0
    jitter: bool = True

    def next_delay(self, attempt: int) -> float: ...
```

### `DeliveryStatus`

```python
class DeliveryStatus(Enum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    DLQ = "DLQ"          # moved to dead-letter queue
```

---

## Broker-specific envelopes

All envelopes extend `EventEnvelope` with broker-specific metadata:

### `KafkaEventEnvelope`

```python
from integration_automation_patterns import KafkaEventEnvelope

@dataclass
class KafkaEventEnvelope(EventEnvelope):
    topic: str = ""
    partition_key: str = ""    # Kafka partition key
    schema_version: str = ""
    dlq_topic: str = ""        # dead-letter queue topic

    @classmethod
    def from_event(cls, event: EventEnvelope, topic: str, partition_key: str = "") -> "KafkaEventEnvelope": ...
    def to_kafka_message(self) -> dict: ...
```

### `SQSEventEnvelope`

```python
from integration_automation_patterns import SQSEventEnvelope

@dataclass
class SQSEventEnvelope(EventEnvelope):
    queue_url: str = ""
    message_group_id: str = ""       # FIFO queues
    message_deduplication_id: str = ""

    @classmethod
    def from_event(cls, event: EventEnvelope, queue_url: str) -> "SQSEventEnvelope": ...
```

### `GCPPubSubEnvelope`

```python
from integration_automation_patterns import GCPPubSubEnvelope

@dataclass
class GCPPubSubEnvelope(EventEnvelope):
    topic: str = ""
    ordering_key: str = ""

    @classmethod
    def from_event(cls, event: EventEnvelope, topic: str) -> "GCPPubSubEnvelope": ...
```

### `AzureServiceBusEnvelope`

```python
from integration_automation_patterns import AzureServiceBusEnvelope

@dataclass
class AzureServiceBusEnvelope(EventEnvelope):
    queue_name: str = ""
    session_id: str = ""
    message_id: str = ""

    @classmethod
    def from_event(cls, event: EventEnvelope, queue_name: str) -> "AzureServiceBusEnvelope": ...
```

---

## Sync boundary

### `SyncBoundary`

Field-level authority model for bi-directional system-of-record sync.

```python
class SyncBoundary:
    def __init__(self, authorities: dict[str, RecordAuthority]): ...

    def detect_conflict(
        self,
        incoming_system: str,
        fields: dict[str, Any],
    ) -> dict[str, SyncConflict]: ...

    def apply_update(
        self,
        incoming_system: str,
        fields: dict[str, Any],
        current_record: dict[str, Any],
    ) -> dict[str, Any]: ...
```

**`detect_conflict`** — Returns a dict of field → `SyncConflict` for every field where `incoming_system` is not the owner.

**`apply_update`** — Returns the updated record, silently dropping fields not owned by `incoming_system`.

### `RecordAuthority`

```python
@dataclass
class RecordAuthority:
    owner: str                  # system of record for this field
    read_others: list[str] = field(default_factory=list)   # other systems allowed to read
    write_others: list[str] = field(default_factory=list)  # other systems allowed to write
```

### `SyncConflict`

```python
@dataclass
class SyncConflict:
    field_name: str
    owner: str
    incoming_system: str
    conflict_type: str    # "AUTHORITY_VIOLATION" | "CONCURRENT_WRITE"
```

---

## Circuit breaker

### `CircuitBreaker`

CLOSED → OPEN → HALF_OPEN state machine. Wraps any callable; raises `CircuitOpenError` when open.

```python
class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
        half_open_max_calls: int = 3,
    ): ...

    def call(self, fn: Callable[[], Any]) -> Any: ...

    @property
    def state(self) -> CircuitState: ...

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"
```

`CircuitOpenError` is raised when the circuit is OPEN. Catch it and implement fallback logic.

---

## Saga pattern

### `SagaOrchestrator`

Orchestrates a multi-step distributed transaction. On failure, runs compensation in reverse order.

```python
class SagaOrchestrator:
    def __init__(self, steps: list[SagaStep]): ...

    def execute(self) -> SagaResult: ...

@dataclass
class SagaStep:
    name: str
    action: Callable[[], Any]
    compensation: Callable[[], None]

@dataclass
class SagaResult:
    success: bool
    completed_steps: list[str]
    failed_step: str | None
    compensated_steps: list[str]
    error: Exception | None
```

If a step raises, compensation runs for all previously completed steps in reverse order.

---

## Transactional outbox

### `OutboxPublisher` / `OutboxProcessor`

Writes events to an outbox table in the same database transaction as domain changes.

```python
class OutboxPublisher:
    def __init__(self, db_connection: Any): ...

    def publish(self, record: OutboxRecord) -> None: ...

class OutboxProcessor:
    def __init__(self, db_connection: Any): ...

    def poll_and_publish(
        self,
        broker_publish_fn: Callable[[OutboxRecord], None],
        batch_size: int = 100,
    ) -> int: ...   # returns count of published records

@dataclass
class OutboxRecord:
    event_id: str
    destination: str       # topic / queue name
    payload: dict
    created_at: str = ""
    status: str = "PENDING"
```

---

## Webhook handler

### `WebhookHandler`

HMAC-SHA256 signature verification + replay protection.

```python
class WebhookHandler:
    def __init__(
        self,
        signing_secret: str,
        dedup_window_seconds: int = 300,
        header_name: str = "X-Webhook-Signature",
    ): ...

    def parse(
        self,
        payload: bytes,
        signature: str,
    ) -> WebhookEvent: ...

@dataclass
class WebhookEvent:
    event_id: str
    event_type: str
    payload: dict
    received_at: str
```

Raises:
- `WebhookSignatureError` — signature does not match
- `WebhookReplayError` — event_id already seen within `dedup_window_seconds`

---

## CDC events

### `CDCEvent`

Change Data Capture event (Debezium-compatible).

```python
@dataclass
class CDCEvent:
    source_table: str
    operation: CDCOperation
    before: dict | None     # state before change (None for INSERT)
    after: dict | None      # state after change (None for DELETE)
    source_metadata: CDCSourceMetadata

class CDCOperation(Enum):
    INSERT = "c"       # Debezium "create"
    UPDATE = "u"
    DELETE = "d"
    SNAPSHOT = "r"     # initial snapshot read

@dataclass
class CDCSourceMetadata:
    connector: str      # "postgresql", "mysql", "mongodb"
    db: str
    table: str
    ts_ms: int          # event timestamp milliseconds
    lsn: int | None     # PostgreSQL LSN
    snapshot: bool
```

---

## Rate limiting

### `SlidingWindowRateLimiter`

Per-client sliding window rate limiter. Thread-safe.

```python
class SlidingWindowRateLimiter:
    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
    ): ...

    def allow(self, client_id: str) -> bool: ...
    def remaining(self, client_id: str) -> int: ...
    def reset_at(self, client_id: str) -> float: ...   # monotonic timestamp
```

### `TokenBucketRateLimiter`

Burst-capable token bucket. Thread-safe.

```python
class TokenBucketRateLimiter:
    def __init__(
        self,
        capacity: int,              # max burst
        refill_rate: float,         # tokens per second
    ): ...

    def consume(self, tokens: int = 1) -> bool: ...   # False if insufficient tokens
    def available_tokens(self) -> float: ...
```

`RateLimitExceeded` is raised by higher-level patterns when rate limit is hit.

---

## MCP security

### `MCPInvocationGuard` — `examples/42_mcp_security_patterns.py`

Guards Model Context Protocol tool invocations against:
- CVE-2025-6514 class command injection via MCP tool metadata
- Malicious MCP package supply-chain attacks (Sept 2025 class)
- Unvetted tool composition (unknown tool origin)

```python
from examples.mcp_security_patterns import (
    MCPToolDefinition,
    MCPSecurityValidator,
    MCPToolRegistry,
    MCPInvocationGuard,
)

guard = MCPInvocationGuard(
    registry=tool_registry,
    validator=validator,
    rate_limiter=MCPRateLimiter(max_per_minute=60),
    audit_log=compliance_log.append,
)
result = guard.invoke("read_database", params={"query": user_query})
```

---

## See also

- [Getting Started](./getting-started.md)
- [Architecture Decision Records](./adr/)
- [Integration patterns with regulated-ai-governance](https://github.com/ashutoshrana/regulated-ai-governance) — governance layer for agentic AI
