"""Reference patterns for reliable enterprise integration and workflow automation."""

__version__ = "0.36.0"

from .azure_servicebus_envelope import AzureServiceBusEnvelope
from .cdc_event import CDCEvent, CDCOperation, CDCSourceMetadata
from .circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState
from .event_envelope import DeliveryStatus, EventEnvelope, RetryPolicy
from .gcp_pubsub_envelope import GCPPubSubEnvelope
from .kafka_envelope import KafkaEventEnvelope
from .outbox import AsyncOutboxProcessor, OutboxProcessor, OutboxRecord
from .rabbitmq_envelope import RabbitMQEnvelope
from .rate_limiter import RateLimitExceeded, SlidingWindowRateLimiter, TokenBucketRateLimiter
from .saga import SagaOrchestrator, SagaResult, SagaStep
from .sqs_envelope import SQSEventEnvelope
from .sync_boundary import RecordAuthority, SyncBoundary, SyncConflict
from .webhook_handler import WebhookEvent, WebhookHandler, WebhookReplayError, WebhookSignatureError

# Alias: OutboxPublisher is the primary outbox dispatch interface
OutboxPublisher = OutboxProcessor

__all__ = [
    # Version
    "__version__",
    # Event handling
    "DeliveryStatus",
    "EventEnvelope",
    "RetryPolicy",
    # Broker-specific envelopes
    "KafkaEventEnvelope",
    "SQSEventEnvelope",
    "AzureServiceBusEnvelope",
    "GCPPubSubEnvelope",
    "RabbitMQEnvelope",
    # Change Data Capture
    "CDCEvent",
    "CDCOperation",
    "CDCSourceMetadata",
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    # Saga pattern
    "SagaOrchestrator",
    "SagaStep",
    "SagaResult",
    # Sync boundary
    "RecordAuthority",
    "SyncBoundary",
    "SyncConflict",
    # Outbox pattern
    "AsyncOutboxProcessor",
    "OutboxProcessor",
    "OutboxPublisher",
    "OutboxRecord",
    # Rate limiting
    "TokenBucketRateLimiter",
    "SlidingWindowRateLimiter",
    "RateLimitExceeded",
    # Webhook handling
    "WebhookHandler",
    "WebhookEvent",
    "WebhookSignatureError",
    "WebhookReplayError",
]
