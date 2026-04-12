"""
http — HTTP transport helpers for enterprise integration patterns.

Provides higher-level webhook handling utilities built on top of the
core ``WebhookHandler`` in the parent package.

Available components
--------------------
- ``IdempotentWebhookReceiver`` — HMAC-verified webhook receiver with
  idempotency key extraction and duplicate suppression.
- ``WebhookRouter`` — FastAPI APIRouter factory for HMAC-verified,
  idempotency-safe webhook endpoints (requires ``fastapi`` extra).
"""

from .webhook_handler import IdempotentWebhookReceiver, WebhookReceiveResult

__all__ = [
    "IdempotentWebhookReceiver",
    "WebhookReceiveResult",
    "WebhookRouter",
]


def __getattr__(name: str) -> object:
    if name == "WebhookRouter":
        from .fastapi_router import WebhookRouter  # lazy — fastapi not always installed

        return WebhookRouter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
