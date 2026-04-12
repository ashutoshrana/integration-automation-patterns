"""
Webhook Receiver with FastAPI
==============================
Demonstrates how to use ``IdempotentWebhookReceiver`` to handle HMAC-signed
webhooks from external systems (GitHub, Salesforce, Stripe, ServiceNow)
in a FastAPI endpoint.

In this example, a ``MockRequest`` stub replaces the real FastAPI ``Request``
class so the pattern is self-contained and runnable without installing FastAPI.
To use in production, replace the stubs with real FastAPI components.

Key capabilities shown:
  1. HMAC-SHA256 signature verification (configurable header name)
  2. Header-based idempotency key extraction (prevents duplicate processing)
  3. In-process LRU duplicate cache (no database round-trip needed)
  4. Structured result handling (is_valid / is_duplicate / payload)

Installation:
    pip install integration-automation-patterns

Production imports (replace stubs below):
    from fastapi import FastAPI, Request, Response
    from integration_automation_patterns.http import IdempotentWebhookReceiver
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from typing import Any

from integration_automation_patterns.http import (
    IdempotentWebhookReceiver,
    WebhookReceiveResult,
)

# ---------------------------------------------------------------------------
# Step 1: Configure the receiver once at application startup.
#
# - secret: load from environment / secret manager, not hardcoded
# - signature_header_name: matches what the sender uses
# - idempotency_header: sender-specific unique delivery ID header
# - tolerance_seconds: max age of an accepted event (replay protection)
# ---------------------------------------------------------------------------

# GitHub webhook configuration
github_receiver = IdempotentWebhookReceiver(
    secret="ghsec_abc123",                  # from os.environ["GITHUB_WEBHOOK_SECRET"]
    signature_header_name="X-Hub-Signature-256",
    idempotency_header="X-GitHub-Delivery",
    tolerance_seconds=300,
)

# Salesforce / generic receiver (different header names)
salesforce_receiver = IdempotentWebhookReceiver(
    secret="sfsec_xyz789",                  # from os.environ["SF_WEBHOOK_SECRET"]
    signature_header_name="X-Salesforce-Signature",
    idempotency_header="X-Salesforce-Delivery-Id",
    tolerance_seconds=300,
)


# ---------------------------------------------------------------------------
# Step 2: Endpoint handler (production: use real FastAPI Request/Response).
# ---------------------------------------------------------------------------

def handle_github_webhook(raw_body: bytes, headers: dict[str, str]) -> dict[str, Any]:
    """
    FastAPI endpoint handler for GitHub webhook events.

    Production version::

        @app.post("/webhooks/github")
        async def github_webhook(request: Request) -> Response:
            body = await request.body()
            return handle_github_webhook(body, dict(request.headers))
    """
    result: WebhookReceiveResult = github_receiver.receive(
        payload=raw_body,
        headers=headers,
    )

    # --- Verification failure ---
    if not result.is_valid:
        return {"status": 400, "body": f"Bad request: {result.error}"}

    # --- Duplicate delivery ---
    if result.is_duplicate:
        print(f"[INFO] Duplicate event suppressed: key={result.idempotency_key!r}")
        return {"status": 200, "body": "duplicate — already processed"}

    # --- New event: process business logic ---
    event_type = result.payload.get("type", "unknown") if result.payload else "unknown"
    print(f"[INFO] Processing new event: type={event_type!r} key={result.idempotency_key!r}")

    # Dispatch to event-specific handler
    if event_type == "push":
        _handle_push_event(result.payload or {})
    elif event_type == "pull_request":
        _handle_pr_event(result.payload or {})
    else:
        print(f"[WARN] Unhandled event type: {event_type!r}")

    return {"status": 200, "body": "ok"}


def _handle_push_event(payload: dict[str, Any]) -> None:
    """Business logic for a GitHub push event."""
    print(f"  [PUSH] ref={payload.get('ref', 'unknown')!r}")


def _handle_pr_event(payload: dict[str, Any]) -> None:
    """Business logic for a GitHub pull_request event."""
    print(f"  [PR] action={payload.get('action', 'unknown')!r}")


# ---------------------------------------------------------------------------
# Step 3: Demo — simulate HTTP requests with correct / incorrect signatures.
# ---------------------------------------------------------------------------

def _make_github_headers(
    body: bytes,
    delivery_id: str,
    secret: str = "ghsec_abc123",
    corrupt_sig: bool = False,
) -> dict[str, str]:
    """Build the HTTP headers that GitHub would send."""
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    sig = f"sha256={digest}" if not corrupt_sig else "sha256=badhex"
    return {
        "Content-Type": "application/json",
        "X-Hub-Signature-256": sig,
        "X-GitHub-Delivery": delivery_id,
        "User-Agent": "GitHub-Hookshot/abc123",
    }


def run_demo() -> None:
    print("=" * 60)
    print("DEMO: IdempotentWebhookReceiver with FastAPI pattern")
    print("=" * 60)

    # --- Scenario 1: valid push event ---
    body1 = json.dumps({"id": "evt_push_1", "type": "push", "ref": "refs/heads/main", "timestamp": time.time()}).encode()
    headers1 = _make_github_headers(body1, delivery_id="github-delivery-001")
    result1 = handle_github_webhook(body1, headers1)
    print(f"Scenario 1 (valid push):        HTTP {result1['status']} — {result1['body']}")

    # --- Scenario 2: duplicate delivery of same event ---
    result2 = handle_github_webhook(body1, headers1)
    print(f"Scenario 2 (duplicate):         HTTP {result2['status']} — {result2['body']}")

    # --- Scenario 3: invalid signature ---
    bad_headers = _make_github_headers(body1, delivery_id="github-delivery-002", corrupt_sig=True)
    result3 = handle_github_webhook(body1, bad_headers)
    print(f"Scenario 3 (bad signature):     HTTP {result3['status']} — {result3['body'][:60]}")

    # --- Scenario 4: valid PR event ---
    body4 = json.dumps({"id": "evt_pr_1", "type": "pull_request", "action": "opened", "timestamp": time.time()}).encode()
    headers4 = _make_github_headers(body4, delivery_id="github-delivery-003")
    result4 = handle_github_webhook(body4, headers4)
    print(f"Scenario 4 (valid PR event):    HTTP {result4['status']} — {result4['body']}")

    # --- Scenario 5: missing signature header ---
    result5 = handle_github_webhook(body4, {"X-GitHub-Delivery": "github-delivery-004"})
    print(f"Scenario 5 (no sig header):     HTTP {result5['status']} — {result5['body'][:60]}")

    print()
    print(f"Seen cache size: {github_receiver.seen_count} unique keys")

    # Verify expected outcomes
    assert result1["status"] == 200
    assert result2["status"] == 200 and "duplicate" in result2["body"]
    assert result3["status"] == 400
    assert result4["status"] == 200
    assert result5["status"] == 400
    print("All assertions passed.")


if __name__ == "__main__":
    run_demo()
else:
    # Run demo when imported as a module (for testing)
    run_demo()
