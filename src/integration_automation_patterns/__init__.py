"""Reference patterns for reliable enterprise integration and workflow automation."""

from .event_envelope import DeliveryStatus, EventEnvelope, RetryPolicy
from .outbox import AsyncOutboxProcessor, OutboxProcessor, OutboxRecord
from .sync_boundary import RecordAuthority, SyncBoundary, SyncConflict

__all__ = [
    # Event handling
    "DeliveryStatus",
    "EventEnvelope",
    "RetryPolicy",
    # Sync boundary
    "RecordAuthority",
    "SyncBoundary",
    "SyncConflict",
    # Outbox pattern
    "AsyncOutboxProcessor",
    "OutboxProcessor",
    "OutboxRecord",
]
