"""
sync_boundary.py — System-of-Record Synchronization Boundaries

Bi-directional synchronization between enterprise systems (CRM and ERP,
for example) fails in predictable ways. The most common failure is
ambiguity about which system is authoritative for a given field when
both systems have updated the same record simultaneously.

This module provides the structural primitives for making synchronization
boundaries explicit. It does not implement a full sync engine — it
provides the objects needed to define authority, detect conflicts, and
route resolution decisions to the right system.

The pattern applies wherever two or more enterprise platforms must
maintain consistent state for overlapping records: CRM + ERP,
CRM + SIS (Student Information System), ERP + ITSM, and similar pairings.
Platform-agnostic — the primitives work regardless of vendor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class RecordAuthority(Enum):
    """
    Defines which system is authoritative for a given field or record type.

    In bi-directional sync, a field must have exactly one authoritative system.
    Changes in the authoritative system propagate outward; changes in
    non-authoritative systems are either rejected, queued for review, or
    overwritten on next sync.

    SHARED indicates that authority is determined per-field (see SyncBoundary)
    rather than per-record. Use this sparingly — field-level authority
    increases complexity.
    """
    SYSTEM_A = "system_a"    # Source system A is authoritative
    SYSTEM_B = "system_b"    # Source system B is authoritative
    SHARED = "shared"        # Field-level authority defined separately
    MANUAL = "manual"        # Human review required before sync


@dataclass(slots=True)
class SyncConflict:
    """
    Represents a detected conflict between two systems' versions of a field.

    A conflict occurs when both systems have modified the same field since
    the last successful sync. The sync boundary policy determines how to
    resolve it: prefer one system, defer to manual review, or apply a
    merge function.

    Attributes:
        field_name: The name of the field where the conflict was detected.
        value_a: The value in system A at detection time.
        value_b: The value in system B at detection time.
        last_sync_value: The agreed-upon value from the last successful sync.
            If None, this is the first sync for this record.
        detected_at: Wall-clock time when the conflict was detected.
        record_id: Identifier of the record where the conflict exists.
        system_a_modified_at: When system A last modified this field.
            None if not available.
        system_b_modified_at: When system B last modified this field.
            None if not available.
    """
    field_name: str
    value_a: Any
    value_b: Any
    record_id: str
    last_sync_value: Any = None
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    system_a_modified_at: datetime | None = None
    system_b_modified_at: datetime | None = None

    def last_writer(self) -> RecordAuthority | None:
        """
        Return which system made the most recent modification, based on
        available modification timestamps.

        Returns None if timestamps are unavailable for one or both systems.
        """
        if self.system_a_modified_at is None or self.system_b_modified_at is None:
            return None
        if self.system_a_modified_at > self.system_b_modified_at:
            return RecordAuthority.SYSTEM_A
        if self.system_b_modified_at > self.system_a_modified_at:
            return RecordAuthority.SYSTEM_B
        return None  # Same timestamp — cannot determine last writer


@dataclass
class SyncBoundary:
    """
    Defines the synchronization contract between two systems for one record type.

    A SyncBoundary specifies:
    1. Which fields exist in both systems
    2. Which system is authoritative for each field
    3. Which fields should never be synced (exclusions)

    Keeping this as an explicit data object — rather than encoding it in
    sync scripts — makes the contract readable, testable, and auditable.

    Usage::

        boundary = SyncBoundary(
            record_type="contact",
            system_a_id="crm",
            system_b_id="erp",
            field_authority={
                "email": RecordAuthority.SYSTEM_A,
                "billing_address": RecordAuthority.SYSTEM_B,
                "preferred_name": RecordAuthority.SYSTEM_A,
                "account_status": RecordAuthority.SYSTEM_B,
            },
            excluded_fields={"internal_notes", "crm_lead_score"},
        )

        # Check if a field is synced and which system wins
        authority = boundary.authority_for("email")   # RecordAuthority.SYSTEM_A
        is_synced = boundary.is_synced("internal_notes")  # False

    Attributes:
        record_type: A name for the type of record this boundary governs.
        system_a_id: Identifier for the first system (e.g., "crm", "salesforce", "hubspot").
        system_b_id: Identifier for the second system (e.g., "erp", "peoplesoft", "sap").
        field_authority: Maps field names to which system is authoritative.
        excluded_fields: Fields that are never synchronized, even if present
            in both systems.
        allow_system_b_override: If True, system B changes to SYSTEM_A-authoritative
            fields are queued for manual review rather than silently discarded.
            Default False (system A changes simply overwrite system B).
    """
    record_type: str
    system_a_id: str
    system_b_id: str
    field_authority: dict[str, RecordAuthority] = field(default_factory=dict)
    excluded_fields: set[str] = field(default_factory=set)
    allow_system_b_override: bool = False

    def authority_for(self, field_name: str) -> RecordAuthority | None:
        """
        Return the authoritative system for the given field.

        Returns None if the field is not in the sync boundary (either
        excluded or not mapped).
        """
        if field_name in self.excluded_fields:
            return None
        return self.field_authority.get(field_name)

    def is_synced(self, field_name: str) -> bool:
        """Return True if this field is part of the synchronization boundary."""
        return field_name not in self.excluded_fields and field_name in self.field_authority

    def synced_fields(self) -> list[str]:
        """Return the list of field names that are actively synchronized."""
        return [f for f in self.field_authority if f not in self.excluded_fields]

    def fields_owned_by(self, authority: RecordAuthority) -> list[str]:
        """Return all fields where the given system is authoritative."""
        return [
            f for f, auth in self.field_authority.items()
            if auth == authority and f not in self.excluded_fields
        ]

    def detect_conflict(
        self,
        field_name: str,
        value_a: Any,
        value_b: Any,
        record_id: str,
        last_sync_value: Any = None,
        system_a_modified_at: datetime | None = None,
        system_b_modified_at: datetime | None = None,
    ) -> SyncConflict | None:
        """
        Detect whether a conflict exists for the given field.

        A conflict exists when both systems have a value different from the
        last sync value, indicating both have modified the field since the
        last sync. If only one system differs from the last sync value,
        that system made the authoritative change and no conflict exists.

        Returns None if no conflict is detected or if the field is not synced.
        """
        if not self.is_synced(field_name):
            return None

        # Both systems agree — no conflict
        if value_a == value_b:
            return None

        # If we have a last sync value, check whether both systems diverged
        if last_sync_value is not None:
            a_changed = value_a != last_sync_value
            b_changed = value_b != last_sync_value
            if not (a_changed and b_changed):
                # Only one system changed — not a conflict
                return None

        return SyncConflict(
            field_name=field_name,
            value_a=value_a,
            value_b=value_b,
            record_id=record_id,
            last_sync_value=last_sync_value,
            system_a_modified_at=system_a_modified_at,
            system_b_modified_at=system_b_modified_at,
        )
