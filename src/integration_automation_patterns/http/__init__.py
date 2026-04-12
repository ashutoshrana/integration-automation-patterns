"""
http — HTTP transport helpers for enterprise integration patterns.

Provides higher-level webhook handling utilities built on top of the
core ``WebhookHandler`` in the parent package.

Available components
--------------------
- ``IdempotentWebhookReceiver`` — wraps ``WebhookHandler`` with
  configurable header-based idempotency key extraction and an
  in-process seen-ID cache for duplicate suppression.
"""

from .webhook_handler import IdempotentWebhookReceiver, WebhookReceiveResult

__all__ = [
    "IdempotentWebhookReceiver",
    "WebhookReceiveResult",
]
