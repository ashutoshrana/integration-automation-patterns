"""Tests for integration_automation_patterns.sync_boundary."""

from datetime import datetime, timezone

import pytest

from integration_automation_patterns.sync_boundary import (
    RecordAuthority,
    SyncBoundary,
    SyncConflict,
)


def _dt(offset_seconds: int = 0) -> datetime:
    from datetime import timedelta
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


def _boundary(**kwargs):
    defaults = dict(
        record_type="contact",
        system_a_id="crm",
        system_b_id="erp",
        field_authority={
            "email": RecordAuthority.SYSTEM_A,
            "billing_address": RecordAuthority.SYSTEM_B,
            "status": RecordAuthority.SYSTEM_A,
        },
        excluded_fields={"internal_notes"},
    )
    defaults.update(kwargs)
    return SyncBoundary(**defaults)


# ---------------------------------------------------------------------------
# SyncBoundary.authority_for
# ---------------------------------------------------------------------------

class TestSyncBoundaryAuthority:
    def test_returns_authority_for_known_field(self):
        b = _boundary()
        assert b.authority_for("email") == RecordAuthority.SYSTEM_A
        assert b.authority_for("billing_address") == RecordAuthority.SYSTEM_B

    def test_returns_none_for_excluded_field(self):
        b = _boundary()
        assert b.authority_for("internal_notes") is None

    def test_returns_none_for_unknown_field(self):
        b = _boundary()
        assert b.authority_for("unknown_field") is None

    def test_is_synced_true_for_mapped_field(self):
        b = _boundary()
        assert b.is_synced("email") is True

    def test_is_synced_false_for_excluded(self):
        b = _boundary()
        assert b.is_synced("internal_notes") is False

    def test_is_synced_false_for_unmapped(self):
        b = _boundary()
        assert b.is_synced("no_such_field") is False

    def test_synced_fields_excludes_excluded(self):
        b = _boundary()
        synced = b.synced_fields()
        assert "internal_notes" not in synced
        assert "email" in synced

    def test_fields_owned_by_system_a(self):
        b = _boundary()
        owned = b.fields_owned_by(RecordAuthority.SYSTEM_A)
        assert "email" in owned
        assert "status" in owned
        assert "billing_address" not in owned

    def test_fields_owned_by_system_b(self):
        b = _boundary()
        owned = b.fields_owned_by(RecordAuthority.SYSTEM_B)
        assert "billing_address" in owned
        assert "email" not in owned


# ---------------------------------------------------------------------------
# SyncBoundary.detect_conflict
# ---------------------------------------------------------------------------

class TestDetectConflict:
    def test_no_conflict_when_values_equal(self):
        b = _boundary()
        result = b.detect_conflict("email", "a@x.com", "a@x.com", "rec-1")
        assert result is None

    def test_conflict_when_both_differ_from_last_sync(self):
        b = _boundary()
        conflict = b.detect_conflict(
            field_name="email",
            value_a="new_a@x.com",
            value_b="new_b@x.com",
            record_id="rec-1",
            last_sync_value="old@x.com",
        )
        assert conflict is not None
        assert isinstance(conflict, SyncConflict)
        assert conflict.field_name == "email"

    def test_no_conflict_when_only_a_changed(self):
        b = _boundary()
        result = b.detect_conflict(
            field_name="email",
            value_a="new_a@x.com",
            value_b="old@x.com",
            record_id="rec-1",
            last_sync_value="old@x.com",
        )
        assert result is None

    def test_no_conflict_when_only_b_changed(self):
        b = _boundary()
        result = b.detect_conflict(
            field_name="email",
            value_a="old@x.com",
            value_b="new_b@x.com",
            record_id="rec-1",
            last_sync_value="old@x.com",
        )
        assert result is None

    def test_conflict_without_last_sync_value(self):
        b = _boundary()
        conflict = b.detect_conflict("email", "a@x.com", "b@x.com", "rec-1")
        assert conflict is not None

    def test_no_conflict_for_excluded_field(self):
        b = _boundary()
        result = b.detect_conflict("internal_notes", "val_a", "val_b", "rec-1")
        assert result is None

    def test_no_conflict_for_unmapped_field(self):
        b = _boundary()
        result = b.detect_conflict("unknown_field", "val_a", "val_b", "rec-1")
        assert result is None

    def test_conflict_contains_correct_values(self):
        b = _boundary()
        conflict = b.detect_conflict(
            "email", "a@x.com", "b@x.com", "rec-42", last_sync_value="old@x.com"
        )
        assert conflict.value_a == "a@x.com"
        assert conflict.value_b == "b@x.com"
        assert conflict.last_sync_value == "old@x.com"
        assert conflict.record_id == "rec-42"


# ---------------------------------------------------------------------------
# SyncConflict.last_writer
# ---------------------------------------------------------------------------

class TestSyncConflictLastWriter:
    def test_last_writer_a_wins(self):
        c = SyncConflict(
            field_name="email",
            value_a="new",
            value_b="old",
            record_id="r1",
            system_a_modified_at=_dt(100),
            system_b_modified_at=_dt(50),
        )
        assert c.last_writer() == RecordAuthority.SYSTEM_A

    def test_last_writer_b_wins(self):
        c = SyncConflict(
            field_name="email",
            value_a="old",
            value_b="new",
            record_id="r1",
            system_a_modified_at=_dt(50),
            system_b_modified_at=_dt(100),
        )
        assert c.last_writer() == RecordAuthority.SYSTEM_B

    def test_last_writer_none_when_timestamps_missing(self):
        c = SyncConflict(field_name="email", value_a="a", value_b="b", record_id="r1")
        assert c.last_writer() is None

    def test_last_writer_none_when_same_timestamp(self):
        ts = _dt(100)
        c = SyncConflict(
            field_name="email",
            value_a="a",
            value_b="b",
            record_id="r1",
            system_a_modified_at=ts,
            system_b_modified_at=ts,
        )
        assert c.last_writer() is None
