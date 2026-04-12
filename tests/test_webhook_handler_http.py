"""
Tests for integration_automation_patterns.http.webhook_handler —
IdempotentWebhookReceiver and WebhookReceiveResult.

Tests are self-contained (no external HTTP framework required).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time

from integration_automation_patterns.http.webhook_handler import (
    IdempotentWebhookReceiver,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SECRET = "test-secret-abc123"
_SIG_HEADER = "X-Signature-256"
_IDEMPOTENCY_HEADER = "X-Idempotency-Key"


def _sign(payload: bytes, secret: str = _SECRET) -> str:
    """Compute sha256=<hex> HMAC for a payload."""
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _make_payload(
    event_id: str = "evt_001",
    event_type: str = "order.created",
    include_timestamp: bool = True,
    extra: dict | None = None,
) -> bytes:
    data: dict = {"id": event_id, "type": event_type}
    if include_timestamp:
        data["timestamp"] = time.time()
    if extra:
        data.update(extra)
    return json.dumps(data).encode()


def _make_receiver(
    secret: str = _SECRET,
    tolerance: float | None = 300.0,
    seen_cache_size: int = 10_000,
    sig_header: str = _SIG_HEADER,
    idempotency_header: str = _IDEMPOTENCY_HEADER,
) -> IdempotentWebhookReceiver:
    return IdempotentWebhookReceiver(
        secret=secret,
        signature_header_name=sig_header,
        idempotency_header=idempotency_header,
        tolerance_seconds=tolerance,
        seen_cache_size=seen_cache_size,
    )


# ---------------------------------------------------------------------------
# Signature verification tests
# ---------------------------------------------------------------------------


class TestSignatureVerification:
    def test_valid_signature_accepted(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload()
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_valid is True
        assert result.error is None

    def test_invalid_signature_rejected(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload()
        result = receiver.receive(payload, {_SIG_HEADER: "sha256=deadbeef"})
        assert result.is_valid is False
        assert result.error is not None

    def test_missing_signature_header_rejected(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload()
        result = receiver.receive(payload, {})
        assert result.is_valid is False
        assert "Missing signature header" in (result.error or "")

    def test_wrong_secret_rejected(self) -> None:
        receiver = _make_receiver(secret="correct-secret")
        payload = _make_payload()
        wrong_sig = _sign(payload, secret="wrong-secret")
        result = receiver.receive(payload, {_SIG_HEADER: wrong_sig})
        assert result.is_valid is False

    def test_header_case_insensitive(self) -> None:
        """Header lookup must be case-insensitive per HTTP spec."""
        receiver = _make_receiver(sig_header="X-Signature-256")
        payload = _make_payload()
        sig = _sign(payload)
        # Send header in lowercase
        result = receiver.receive(payload, {"x-signature-256": sig})
        assert result.is_valid is True

    def test_v1_format_signature_accepted(self) -> None:
        """Stripe-style v1=<hex> signature format must be accepted."""
        receiver = _make_receiver()
        payload = _make_payload()
        digest = hmac.new(_SECRET.encode(), payload, hashlib.sha256).hexdigest()
        result = receiver.receive(payload, {_SIG_HEADER: f"v1={digest}"})
        assert result.is_valid is True


# ---------------------------------------------------------------------------
# Replay protection tests
# ---------------------------------------------------------------------------


class TestReplayProtection:
    def test_stale_timestamp_rejected(self) -> None:
        receiver = _make_receiver(tolerance=60.0)
        payload = json.dumps(
            {
                "id": "evt_stale",
                "type": "test",
                "timestamp": time.time() - 120,  # 2 minutes old
            }
        ).encode()
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_valid is False
        assert result.error is not None

    def test_no_timestamp_not_checked(self) -> None:
        """Payloads without a timestamp field skip replay protection."""
        receiver = _make_receiver(tolerance=60.0)
        payload = _make_payload(include_timestamp=False)
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_valid is True

    def test_tolerance_none_disables_check(self) -> None:
        receiver = _make_receiver(tolerance=None)
        payload = json.dumps(
            {
                "id": "evt_old",
                "type": "test",
                "timestamp": time.time() - 99999,
            }
        ).encode()
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_valid is True


# ---------------------------------------------------------------------------
# Idempotency key extraction
# ---------------------------------------------------------------------------


class TestIdempotencyKeyExtraction:
    def test_key_extracted_from_header(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_001")
        sig = _sign(payload)
        result = receiver.receive(
            payload,
            {_SIG_HEADER: sig, _IDEMPOTENCY_HEADER: "unique-delivery-id-xyz"},
        )
        assert result.idempotency_key == "unique-delivery-id-xyz"

    def test_key_falls_back_to_event_id_when_header_absent(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_fallback")
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.idempotency_key == "evt_fallback"

    def test_key_is_none_when_both_absent(self) -> None:
        receiver = _make_receiver()
        payload = json.dumps({"type": "test", "timestamp": time.time()}).encode()
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.idempotency_key is None

    def test_custom_idempotency_header(self) -> None:
        receiver = _make_receiver(idempotency_header="X-GitHub-Delivery")
        payload = _make_payload()
        sig = _sign(payload)
        result = receiver.receive(
            payload,
            {_SIG_HEADER: sig, "X-GitHub-Delivery": "github-delivery-abc"},
        )
        assert result.idempotency_key == "github-delivery-abc"

    def test_idempotency_key_case_insensitive(self) -> None:
        """Header name lookup for idempotency key must be case-insensitive."""
        receiver = _make_receiver(idempotency_header="X-Idempotency-Key")
        payload = _make_payload()
        sig = _sign(payload)
        result = receiver.receive(
            payload,
            {_SIG_HEADER: sig, "x-idempotency-key": "lower-case-key"},
        )
        assert result.idempotency_key == "lower-case-key"


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------


class TestDuplicateDetection:
    def test_first_delivery_not_duplicate(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_new")
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_duplicate is False

    def test_second_delivery_of_same_key_is_duplicate(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_dup")
        sig = _sign(payload)
        receiver.receive(payload, {_SIG_HEADER: sig})  # first delivery
        result = receiver.receive(payload, {_SIG_HEADER: sig})  # second delivery
        assert result.is_duplicate is True
        assert result.is_valid is True  # still valid — just duplicate

    def test_different_keys_not_duplicate(self) -> None:
        receiver = _make_receiver()
        for i in range(5):
            payload = _make_payload(event_id=f"evt_{i}")
            sig = _sign(payload)
            result = receiver.receive(payload, {_SIG_HEADER: sig})
            assert result.is_duplicate is False

    def test_header_key_deduplicates_across_different_payloads(self) -> None:
        """Same idempotency header key on a different payload is still a duplicate."""
        receiver = _make_receiver()
        p1 = _make_payload(event_id="evt_a", event_type="order.created")
        p2 = _make_payload(event_id="evt_b", event_type="order.updated")
        sig1, sig2 = _sign(p1), _sign(p2)
        ikey = "shared-idempotency-key"

        receiver.receive(p1, {_SIG_HEADER: sig1, _IDEMPOTENCY_HEADER: ikey})
        result = receiver.receive(p2, {_SIG_HEADER: sig2, _IDEMPOTENCY_HEADER: ikey})
        assert result.is_duplicate is True

    def test_invalid_signature_does_not_populate_seen_cache(self) -> None:
        """Failed verification should not mark the key as seen."""
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_bad")
        receiver.receive(payload, {_SIG_HEADER: "sha256=bad"})  # fails
        assert receiver.seen_count == 0

        # Now send with correct sig — should NOT be a duplicate
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_valid is True
        assert result.is_duplicate is False


# ---------------------------------------------------------------------------
# Cache capacity / LRU eviction
# ---------------------------------------------------------------------------


class TestCacheCapacity:
    def test_cache_bounded_by_seen_cache_size(self) -> None:
        receiver = _make_receiver(seen_cache_size=5, tolerance=None)
        for i in range(7):
            payload = json.dumps({"id": f"evt_{i}", "type": "test"}).encode()
            sig = _sign(payload)
            receiver.receive(payload, {_SIG_HEADER: sig})
        assert receiver.seen_count == 5

    def test_oldest_entry_evicted_first(self) -> None:
        """After eviction, the first key should no longer be in the cache,
        so a re-delivery of evt_0 is NOT marked as duplicate."""
        receiver = _make_receiver(seen_cache_size=3, tolerance=None)
        for i in range(4):  # overfill: evt_0 should be evicted
            payload = json.dumps({"id": f"evt_{i}", "type": "test"}).encode()
            sig = _sign(payload)
            receiver.receive(payload, {_SIG_HEADER: sig})

        # Re-deliver evt_0 — it was evicted, so should NOT be a duplicate
        p0 = json.dumps({"id": "evt_0", "type": "test"}).encode()
        result = receiver.receive(p0, {_SIG_HEADER: _sign(p0)})
        assert result.is_duplicate is False

    def test_clear_seen_cache(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_x")
        sig = _sign(payload)
        receiver.receive(payload, {_SIG_HEADER: sig})
        assert receiver.seen_count == 1

        receiver.clear_seen_cache()
        assert receiver.seen_count == 0

        # After clearing, re-delivery is not a duplicate
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.is_duplicate is False


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------


class TestPayloadParsing:
    def test_parsed_payload_in_result(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_001", event_type="order.created", extra={"amount": 42})
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.payload is not None
        assert result.payload["amount"] == 42

    def test_event_in_result(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload(event_id="evt_001", event_type="order.created")
        sig = _sign(payload)
        result = receiver.receive(payload, {_SIG_HEADER: sig})
        assert result.event is not None
        assert result.event.event_id == "evt_001"
        assert result.event.event_type == "order.created"

    def test_payload_none_on_failure(self) -> None:
        receiver = _make_receiver()
        payload = _make_payload()
        result = receiver.receive(payload, {_SIG_HEADER: "sha256=bad"})
        assert result.payload is None
        assert result.event is None
