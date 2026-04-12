"""
http/webhook_handler.py — Idempotent webhook receiver for enterprise integrations.

Extends the core ``WebhookHandler`` (HMAC-SHA256 signature verification) with:
  - **Header-based idempotency key extraction**: configurable header name for
    extracting the unique event ID supplied by the sender (e.g. Salesforce
    ``X-Salesforce-Delivery-Id``, Stripe ``Stripe-Idempotency-Key``, GitHub
    ``X-GitHub-Delivery``).
  - **Duplicate suppression**: an in-process LRU-style seen-ID cache that
    rejects replayed events without requiring a database round-trip.
  - **Structured result**: ``WebhookReceiveResult`` carries the verification
    outcome, idempotency key, parsed payload, and a ``is_duplicate`` flag so
    callers can handle idempotent skips distinctly from new events.

Typical usage in an HTTP endpoint (e.g. FastAPI, Flask, Django)::

    from integration_automation_patterns.http import IdempotentWebhookReceiver

    receiver = IdempotentWebhookReceiver(
        secret="whsec_abc123",
        idempotency_header="X-GitHub-Delivery",
        signature_header_name="X-Hub-Signature-256",
    )

    @app.post("/webhooks/github")
    async def github_webhook(request: Request):
        body = await request.body()
        result = receiver.receive(
            payload=body,
            headers=dict(request.headers),
        )
        if not result.is_valid:
            return Response(status_code=400, content=result.error)
        if result.is_duplicate:
            return Response(status_code=200, content="duplicate — already processed")
        process_event(result.payload)
        return Response(status_code=200)
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from integration_automation_patterns.webhook_handler import (
    WebhookEvent,
    WebhookHandler,
    WebhookReplayError,
    WebhookSignatureError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public re-exports so callers can import from the http subpackage directly
# ---------------------------------------------------------------------------

__all__ = [
    "IdempotentWebhookReceiver",
    "WebhookReceiveResult",
    # Core types re-exported for convenience
    "WebhookEvent",
    "WebhookSignatureError",
    "WebhookReplayError",
]


@dataclass
class WebhookReceiveResult:
    """
    Structured result from ``IdempotentWebhookReceiver.receive``.

    Attributes:
        is_valid: ``True`` when HMAC signature verification passed and the
            event timestamp is within the replay-protection window.
        is_duplicate: ``True`` when the idempotency key has been seen before.
            A duplicate result is still ``is_valid=True`` — it passed
            verification but should be skipped in business logic.
        idempotency_key: The unique event identifier extracted from the
            configured idempotency header.  ``None`` when the header is
            absent.
        payload: Parsed JSON payload dict.  ``None`` on verification failure.
        event: The underlying ``WebhookEvent`` from the core handler.
            ``None`` on verification failure.
        error: Human-readable error message when ``is_valid=False``.
    """

    is_valid: bool
    is_duplicate: bool = False
    idempotency_key: str | None = None
    payload: dict[str, Any] | None = None
    event: WebhookEvent | None = None
    error: str | None = None


class IdempotentWebhookReceiver:
    """
    HMAC-verified webhook receiver with header-based idempotency key
    extraction and in-process duplicate suppression.

    Wraps ``WebhookHandler`` from the parent package and adds:

    1. **Idempotency key extraction**: reads the configured
       ``idempotency_header`` from the incoming HTTP headers, e.g.:
       - GitHub: ``X-GitHub-Delivery``
       - Salesforce: ``X-Salesforce-Delivery-Id``
       - Stripe: ``Idempotency-Key``
       - Generic: ``X-Idempotency-Key``

    2. **Duplicate suppression**: maintains an in-process ordered dict
       of seen idempotency keys (capacity-bounded by ``seen_cache_size``).
       Events with a previously seen key are returned with ``is_duplicate=True``
       without re-running business logic.

    3. **Structured result**: always returns ``WebhookReceiveResult`` — never
       raises — so callers can handle verification failures, duplicates, and
       new events with a single conditional branch.

    Args:
        secret: Shared HMAC-SHA256 secret from the sender.
        signature_header_name: HTTP header carrying the signature.
            Default: ``"X-Signature-256"``
        idempotency_header: HTTP header carrying the unique event ID.
            Default: ``"X-Idempotency-Key"``
        tolerance_seconds: Replay-protection window in seconds.
            ``None`` disables replay protection.
        seen_cache_size: Maximum number of idempotency keys to retain in
            the in-process duplicate cache.  When the cache is full, the
            oldest entry is evicted (LRU-style).  Default: 10,000.
        timestamp_field: JSON body field for the event timestamp.
        id_field: JSON body field for the event ID.
        type_field: JSON body field for the event type discriminator.
    """

    def __init__(
        self,
        secret: str,
        signature_header_name: str = "X-Signature-256",
        idempotency_header: str = "X-Idempotency-Key",
        tolerance_seconds: float | None = 300.0,
        seen_cache_size: int = 10_000,
        timestamp_field: str = "timestamp",
        id_field: str = "id",
        type_field: str = "type",
    ) -> None:
        self._handler = WebhookHandler(
            secret=secret,
            tolerance_seconds=tolerance_seconds,
            timestamp_field=timestamp_field,
            id_field=id_field,
            type_field=type_field,
        )
        self.signature_header_name = signature_header_name
        self.idempotency_header = idempotency_header
        self._seen: OrderedDict[str, bool] = OrderedDict()
        self._seen_cache_size = seen_cache_size

    def receive(
        self,
        payload: bytes,
        headers: dict[str, str],
    ) -> WebhookReceiveResult:
        """
        Verify the webhook signature and check for duplicate delivery.

        Args:
            payload: Raw request body bytes (not pre-decoded).
            headers: HTTP request headers dict.  Header name lookup is
                case-insensitive.

        Returns:
            ``WebhookReceiveResult`` with ``is_valid``, ``is_duplicate``,
            ``idempotency_key``, ``payload``, ``event``, and ``error`` fields.
        """
        # Normalise headers to lowercase for case-insensitive lookup
        normalised = {k.lower(): v for k, v in headers.items()}

        # Extract idempotency key from headers
        idempotency_key = normalised.get(self.idempotency_header.lower())

        # Extract signature header
        signature = normalised.get(self.signature_header_name.lower())
        if not signature:
            return WebhookReceiveResult(
                is_valid=False,
                idempotency_key=idempotency_key,
                error=(
                    f"Missing signature header: {self.signature_header_name!r}. "
                    "Ensure the sender includes an HMAC-SHA256 signature."
                ),
            )

        # Verify HMAC and timestamp
        try:
            event = self._handler.parse(payload=payload, signature_header=signature)
        except WebhookSignatureError as exc:
            logger.warning("Webhook signature verification failed: %s", exc)
            return WebhookReceiveResult(
                is_valid=False,
                idempotency_key=idempotency_key,
                error=str(exc),
            )
        except WebhookReplayError as exc:
            logger.warning("Webhook replay attack detected: %s", exc)
            return WebhookReceiveResult(
                is_valid=False,
                idempotency_key=idempotency_key,
                error=str(exc),
            )

        # Determine effective idempotency key: header value takes priority,
        # fall back to the event_id extracted from the JSON body.
        effective_key = idempotency_key or (event.event_id if event.event_id else None)

        # Check for duplicates
        is_duplicate = False
        if effective_key:
            if effective_key in self._seen:
                is_duplicate = True
                logger.info(
                    "Duplicate webhook event suppressed: key=%r type=%r",
                    effective_key,
                    event.event_type,
                )
            else:
                self._record_seen(effective_key)

        return WebhookReceiveResult(
            is_valid=True,
            is_duplicate=is_duplicate,
            idempotency_key=effective_key,
            payload=event.payload,
            event=event,
        )

    def _record_seen(self, key: str) -> None:
        """Add *key* to the seen cache, evicting the oldest entry if at capacity."""
        if len(self._seen) >= self._seen_cache_size:
            self._seen.popitem(last=False)  # LRU eviction
        self._seen[key] = True

    @property
    def seen_count(self) -> int:
        """Number of unique idempotency keys currently in the seen cache."""
        return len(self._seen)

    def clear_seen_cache(self) -> None:
        """Flush the in-process duplicate cache (e.g., on test reset)."""
        self._seen.clear()
