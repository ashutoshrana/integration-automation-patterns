"""
webhook_handler.py — HMAC-signed webhook handler for enterprise integrations.

Verifies incoming webhooks from external systems (Salesforce, Stripe, GitHub,
ServiceNow) that use HMAC-SHA256 signature verification.

Pattern:
  1. Receive raw request body + signature header
  2. Verify HMAC-SHA256 of body against shared secret
  3. Parse and dispatch to typed handler

Usage::

    handler = WebhookHandler(secret="whsec_abc123", tolerance_seconds=300)

    # In your HTTP endpoint:
    try:
        event = handler.parse(
            payload=request.body,            # raw bytes
            signature_header=request.headers["X-Signature-256"],
        )
        process_event(event)
    except WebhookSignatureError as e:
        return HttpResponse(status=400, body=str(e))
    except WebhookReplayError as e:
        return HttpResponse(status=400, body=str(e))
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from typing import Any


class WebhookSignatureError(ValueError):
    """Raised when HMAC signature verification fails."""


class WebhookReplayError(ValueError):
    """Raised when the event timestamp is outside the tolerance window."""


@dataclass
class WebhookEvent:
    """
    A verified, parsed webhook event.

    Attributes:
        event_id: Unique identifier extracted from the payload (``id`` field).
        event_type: Event discriminator (``type`` or ``event_type`` field).
        payload: Full parsed payload dict.
        received_at: Unix timestamp when ``parse()`` was called.
        raw: Original raw bytes of the request body.
    """

    event_id: str
    event_type: str
    payload: dict[str, Any]
    received_at: float = field(default_factory=time.time)
    raw: bytes = field(default=b"", repr=False)


@dataclass
class WebhookHandler:
    """
    Validates and parses HMAC-SHA256 signed webhook payloads.

    Compatible with Stripe (``Stripe-Signature``), GitHub (``X-Hub-Signature-256``),
    Salesforce, and any system using ``sha256=<hex>`` or ``v1=<hex>`` signature format.

    Args:
        secret: Shared secret used by the sending system to sign payloads.
        tolerance_seconds: Maximum age (in seconds) of an accepted event.
            Set to ``None`` to disable replay protection (not recommended for production).
        timestamp_field: JSON field name for the event timestamp.
            If the payload does not contain this field, replay protection is skipped.
        id_field: JSON field name for the event ID.
        type_field: JSON field name for the event type discriminator.
    """

    secret: str
    tolerance_seconds: float | None = 300.0
    timestamp_field: str = "timestamp"
    id_field: str = "id"
    type_field: str = "type"

    def parse(self, payload: bytes, signature_header: str) -> WebhookEvent:
        """
        Verify signature and parse a webhook payload.

        Args:
            payload: Raw request body bytes (do not decode before passing).
            signature_header: Value of the signature header (e.g.
                ``"sha256=abc123..."`` or ``"t=1234,v1=abc123..."``).

        Returns:
            A ``WebhookEvent`` if verification succeeds.

        Raises:
            WebhookSignatureError: If the HMAC signature is invalid.
            WebhookReplayError: If the event timestamp is stale.
        """
        expected = self._compute_hmac(payload)
        received = self._extract_signature(signature_header)

        if not hmac.compare_digest(expected, received):
            raise WebhookSignatureError(
                "Webhook signature verification failed. "
                "Ensure the shared secret matches the sending system's configuration."
            )

        data: dict[str, Any] = json.loads(payload)

        if self.tolerance_seconds is not None:
            ts = data.get(self.timestamp_field)
            if ts is not None:
                age = time.time() - float(ts)
                if abs(age) > self.tolerance_seconds:
                    raise WebhookReplayError(
                        f"Event timestamp is {age:.0f}s old; tolerance is {self.tolerance_seconds}s. "
                        "This may be a replay attack."
                    )

        return WebhookEvent(
            event_id=str(data.get(self.id_field, "")),
            event_type=str(data.get(self.type_field, "unknown")),
            payload=data,
            raw=payload,
        )

    def _compute_hmac(self, payload: bytes) -> str:
        """Compute HMAC-SHA256 of *payload* using ``self.secret``."""
        return hmac.new(
            self.secret.encode("utf-8"),
            payload,
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def _extract_signature(header: str) -> str:
        """
        Extract the hex signature from a header value.

        Handles formats:
        - ``"sha256=<hex>"`` (GitHub, generic)
        - ``"v1=<hex>"`` (Stripe-style)
        - ``"<hex>"`` (bare hex)
        - ``"t=<ts>,v1=<hex>"`` (Stripe with timestamp prefix)
        """
        for part in header.split(","):
            part = part.strip()
            for prefix in ("sha256=", "v1="):
                if part.startswith(prefix):
                    return part[len(prefix) :]
        # Bare hex or unrecognised format — return as-is
        return header.strip()
