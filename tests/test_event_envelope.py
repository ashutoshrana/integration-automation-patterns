"""Tests for integration_automation_patterns.event_envelope."""


from integration_automation_patterns.event_envelope import (
    DeliveryStatus,
    EventEnvelope,
    RetryPolicy,
)

# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------

class TestRetryPolicy:
    def test_default_values(self):
        rp = RetryPolicy()
        assert rp.max_attempts == 3
        assert rp.backoff_seconds == 30
        assert rp.exponential is True
        assert rp.max_backoff_seconds == 3600

    def test_is_exhausted_at_limit(self):
        rp = RetryPolicy(max_attempts=3)
        assert rp.is_exhausted(3) is True

    def test_is_not_exhausted_below_limit(self):
        rp = RetryPolicy(max_attempts=3)
        assert rp.is_exhausted(2) is False

    def test_fixed_backoff(self):
        rp = RetryPolicy(backoff_seconds=10, exponential=False)
        assert rp.wait_seconds_for_attempt(0) == 10
        assert rp.wait_seconds_for_attempt(5) == 10

    def test_exponential_backoff(self):
        rp = RetryPolicy(backoff_seconds=10, exponential=True, max_backoff_seconds=9999)
        assert rp.wait_seconds_for_attempt(0) == 10   # 10 * 2^0
        assert rp.wait_seconds_for_attempt(1) == 20   # 10 * 2^1
        assert rp.wait_seconds_for_attempt(2) == 40   # 10 * 2^2

    def test_exponential_backoff_capped(self):
        rp = RetryPolicy(backoff_seconds=10, exponential=True, max_backoff_seconds=50)
        assert rp.wait_seconds_for_attempt(10) == 50  # capped at max


# ---------------------------------------------------------------------------
# EventEnvelope lifecycle
# ---------------------------------------------------------------------------

def _make_event(**kwargs):
    defaults = dict(
        event_id="evt-001",
        event_type="enrollment.student.updated",
        source_system="crm",
        payload={"student_id": "S-1"},
    )
    defaults.update(kwargs)
    return EventEnvelope(**defaults)


class TestEventEnvelopeInitial:
    def test_initial_status_pending(self):
        ev = _make_event()
        assert ev.status == DeliveryStatus.PENDING

    def test_initial_attempt_count_zero(self):
        ev = _make_event()
        assert ev.attempt_count == 0

    def test_is_not_terminal_when_pending(self):
        ev = _make_event()
        assert ev.is_terminal() is False

    def test_fields_stored(self):
        ev = _make_event(correlation_id="corr-99", schema_version="2.0")
        assert ev.event_id == "evt-001"
        assert ev.event_type == "enrollment.student.updated"
        assert ev.source_system == "crm"
        assert ev.correlation_id == "corr-99"
        assert ev.schema_version == "2.0"


class TestEventEnvelopeTransitions:
    def test_mark_dispatched_increments_attempt(self):
        ev = _make_event()
        ev.mark_dispatched()
        assert ev.attempt_count == 1
        assert ev.status == DeliveryStatus.DISPATCHED

    def test_mark_acknowledged(self):
        ev = _make_event()
        ev.mark_dispatched()
        ev.mark_acknowledged()
        assert ev.status == DeliveryStatus.ACKNOWLEDGED

    def test_mark_processed_is_terminal(self):
        ev = _make_event()
        ev.mark_dispatched()
        ev.mark_processed()
        assert ev.status == DeliveryStatus.PROCESSED
        assert ev.is_terminal() is True

    def test_mark_failed_within_budget_sets_retrying(self):
        ev = _make_event()
        ev.mark_dispatched()   # attempt_count = 1
        ev.mark_failed()       # attempt_count=1 < max_attempts=3 → RETRYING
        assert ev.status == DeliveryStatus.RETRYING
        assert ev.is_terminal() is False

    def test_mark_failed_budget_exhausted_sets_failed(self):
        ev = _make_event()
        for _ in range(3):     # exhaust budget
            ev.mark_dispatched()
        ev.mark_failed()
        assert ev.status == DeliveryStatus.FAILED
        assert ev.is_terminal() is True

    def test_mark_skipped_is_terminal(self):
        ev = _make_event()
        ev.mark_skipped(reason="duplicate")
        assert ev.status == DeliveryStatus.SKIPPED
        assert ev.is_terminal() is True
        assert ev.tags["skip_reason"] == "duplicate"

    def test_mark_skipped_without_reason(self):
        ev = _make_event()
        ev.mark_skipped()
        assert ev.status == DeliveryStatus.SKIPPED
        assert "skip_reason" not in ev.tags

    def test_next_retry_wait_zero_when_not_retrying(self):
        ev = _make_event()
        assert ev.next_retry_wait_seconds() == 0

    def test_next_retry_wait_positive_when_retrying(self):
        ev = _make_event()
        ev.mark_dispatched()
        ev.mark_failed()
        assert ev.next_retry_wait_seconds() > 0


class TestEventEnvelopeAudit:
    def test_to_audit_line_contains_key_fields(self):
        ev = _make_event(correlation_id="corr-1")
        ev.mark_dispatched()
        line = ev.to_audit_line()
        assert "[INTEGRATION_EVENT]" in line
        assert "event_id=evt-001" in line
        assert "type=enrollment.student.updated" in line
        assert "source=crm" in line
        assert "correlation=corr-1" in line

    def test_audit_line_no_correlation(self):
        ev = _make_event()
        line = ev.to_audit_line()
        assert "correlation=none" in line
