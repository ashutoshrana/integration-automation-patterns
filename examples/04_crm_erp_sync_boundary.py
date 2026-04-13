"""
04_crm_erp_sync_boundary.py — CRM-ERP bi-directional sync with conflict resolution.

Demonstrates the SyncBoundary pattern for synchronizing Account records between
Salesforce (CRM) and SAP S/4HANA (ERP). Each system is authoritative for
different fields; conflicts arise when both systems update the same field
between sync cycles.

This example walks through three conflict resolution strategies:
  1. Authority-wins — the owning system's value is always applied
  2. Last-writer-wins — the most recently modified value is applied
  3. Manual-review — the conflict is escalated for human resolution

Run:
    python examples/04_crm_erp_sync_boundary.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from integration_automation_patterns.sync_boundary import (
    RecordAuthority,
    SyncBoundary,
    SyncConflict,
)

# ---------------------------------------------------------------------------
# Field authority design for Account / Business Partner
# ---------------------------------------------------------------------------
#
# Salesforce is the master for revenue-facing fields: account name,
# primary contact email, opportunity stage, and CRM segment.
#
# SAP S/4HANA is the master for financial fields: billing address, payment
# terms, credit limit, and ERP account code.  Legal entity name is shared
# — finance and CRM sometimes update it independently; we flag those for
# manual review.
#
# Fields that exist only in one system (SF lead score, SAP cost center) are
# excluded from the sync boundary entirely.

ACCOUNT_BOUNDARY = SyncBoundary(
    record_type="account",
    system_a_id="salesforce",
    system_b_id="sap_s4",
    field_authority={
        # Salesforce-owned (propagate SF changes to SAP)
        "account_name": RecordAuthority.SYSTEM_A,
        "primary_email": RecordAuthority.SYSTEM_A,
        "crm_segment": RecordAuthority.SYSTEM_A,
        "account_status": RecordAuthority.SYSTEM_A,
        # SAP-owned (propagate SAP changes to SF)
        "billing_street": RecordAuthority.SYSTEM_B,
        "billing_city": RecordAuthority.SYSTEM_B,
        "billing_country": RecordAuthority.SYSTEM_B,
        "payment_terms": RecordAuthority.SYSTEM_B,
        "credit_limit_usd": RecordAuthority.SYSTEM_B,
        "erp_account_code": RecordAuthority.SYSTEM_B,
        # Shared — requires manual review on conflict
        "legal_entity_name": RecordAuthority.SHARED,
        "vat_registration_number": RecordAuthority.SHARED,
    },
    excluded_fields={
        "sf_lead_score",  # SF only — not synced to SAP
        "sap_cost_center",  # SAP only — not synced to SF
        "sf_internal_notes",  # SF only — not synced
        "sap_dunning_level",  # SAP internal — not synced
    },
    allow_system_b_override=True,  # SAP changes to SF-owned fields queued for review
)


# ---------------------------------------------------------------------------
# Simulated record snapshots (state at last sync + current state)
# ---------------------------------------------------------------------------

LAST_SYNC_TIME = datetime(2026, 4, 12, 8, 0, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 4, 12, 14, 30, 0, tzinfo=timezone.utc)

# Last agreed-upon values from previous sync run
LAST_SYNC_VALUES: dict[str, Any] = {
    "account_name": "Meridian Healthcare Systems",
    "primary_email": "ap@meridian-health.com",
    "crm_segment": "enterprise",
    "account_status": "active",
    "billing_street": "450 Healthcare Blvd",
    "billing_city": "Nashville",
    "billing_country": "US",
    "payment_terms": "NET30",
    "credit_limit_usd": 500_000,
    "erp_account_code": "C-8821-00",
    "legal_entity_name": "Meridian Healthcare Systems LLC",
    "vat_registration_number": None,
}

# Current state in Salesforce (sales team updated segment and email; legal name
# was updated by an admin after an acquisition announcement)
SF_CURRENT: dict[str, Any] = {
    "account_name": "Meridian Healthcare Systems",  # unchanged
    "primary_email": "vendor@meridian-health.com",  # SF updated
    "crm_segment": "strategic",  # SF updated (upsell)
    "account_status": "active",  # unchanged
    "billing_street": "450 Healthcare Blvd",  # unchanged
    "billing_city": "Nashville",  # unchanged
    "billing_country": "US",  # unchanged
    "payment_terms": "NET30",  # unchanged
    "credit_limit_usd": 500_000,  # unchanged
    "erp_account_code": "C-8821-00",  # unchanged
    "legal_entity_name": "Meridian Health Systems LLC",  # SF updated (typo fix)
    "vat_registration_number": None,
    # SF-only fields (excluded from boundary)
    "sf_lead_score": 92,
    "sf_internal_notes": "Renewal in Q3 — priority account",
}

SF_FIELD_TIMESTAMPS: dict[str, datetime] = {
    "primary_email": LAST_SYNC_TIME + timedelta(hours=2),
    "crm_segment": LAST_SYNC_TIME + timedelta(hours=3),
    "legal_entity_name": LAST_SYNC_TIME + timedelta(hours=1, minutes=30),
}

# Current state in SAP (finance updated credit limit and billing city after
# office move; legal name was also updated by ERP admin independently)
SAP_CURRENT: dict[str, Any] = {
    "account_name": "Meridian Healthcare Systems",  # unchanged
    "primary_email": "ap@meridian-health.com",  # unchanged (SF owns this)
    "crm_segment": "enterprise",  # SAP mirrors SF; unchanged
    "account_status": "active",  # unchanged
    "billing_street": "450 Healthcare Blvd",  # unchanged
    "billing_city": "Brentwood",  # SAP updated (office moved)
    "billing_country": "US",  # unchanged
    "payment_terms": "NET45",  # SAP updated (renegotiated)
    "credit_limit_usd": 750_000,  # SAP updated (credit review)
    "erp_account_code": "C-8821-00",  # unchanged
    "legal_entity_name": "Meridian Healthcare Systems, LLC",  # SAP updated (comma added)
    "vat_registration_number": None,
    # SAP-only fields (excluded from boundary)
    "sap_cost_center": "CC-2200",
    "sap_dunning_level": 0,
}

SAP_FIELD_TIMESTAMPS: dict[str, datetime] = {
    "billing_city": LAST_SYNC_TIME + timedelta(hours=4),
    "payment_terms": LAST_SYNC_TIME + timedelta(hours=5),
    "credit_limit_usd": LAST_SYNC_TIME + timedelta(hours=5, minutes=15),
    "legal_entity_name": LAST_SYNC_TIME + timedelta(hours=2, minutes=45),
}


# ---------------------------------------------------------------------------
# Conflict resolution strategies
# ---------------------------------------------------------------------------


@dataclass
class SyncDecision:
    field_name: str
    resolution_strategy: str
    winning_value: Any
    winning_system: str
    requires_manual_review: bool = False
    notes: str = ""


def resolve_authority_wins(conflict: SyncConflict, authority: RecordAuthority) -> SyncDecision:
    """The system that owns the field always wins, regardless of timestamps."""
    if authority == RecordAuthority.SYSTEM_A:
        return SyncDecision(
            field_name=conflict.field_name,
            resolution_strategy="authority-wins",
            winning_value=conflict.value_a,
            winning_system="salesforce",
            notes="SF owns this field — SAP value overwritten",
        )
    return SyncDecision(
        field_name=conflict.field_name,
        resolution_strategy="authority-wins",
        winning_value=conflict.value_b,
        winning_system="sap_s4",
        notes="SAP owns this field — SF value overwritten",
    )


def resolve_last_writer_wins(conflict: SyncConflict) -> SyncDecision:
    """The system that made the most recent change wins."""
    last_writer = conflict.last_writer()
    if last_writer == RecordAuthority.SYSTEM_A:
        return SyncDecision(
            field_name=conflict.field_name,
            resolution_strategy="last-writer-wins",
            winning_value=conflict.value_a,
            winning_system="salesforce",
            notes=f"SF modified at {conflict.system_a_modified_at}",
        )
    if last_writer == RecordAuthority.SYSTEM_B:
        return SyncDecision(
            field_name=conflict.field_name,
            resolution_strategy="last-writer-wins",
            winning_value=conflict.value_b,
            winning_system="sap_s4",
            notes=f"SAP modified at {conflict.system_b_modified_at}",
        )
    return SyncDecision(
        field_name=conflict.field_name,
        resolution_strategy="manual-review",
        winning_value=None,
        winning_system="none",
        requires_manual_review=True,
        notes="Timestamps unavailable or equal — cannot determine last writer",
    )


def resolve_manual_review(conflict: SyncConflict) -> SyncDecision:
    """Escalate to human review — sync is blocked for this field until resolved."""
    return SyncDecision(
        field_name=conflict.field_name,
        resolution_strategy="manual-review",
        winning_value=None,
        winning_system="none",
        requires_manual_review=True,
        notes=(f"Shared field conflict: SF='{conflict.value_a}' vs SAP='{conflict.value_b}' — human review required"),
    )


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------


def run_sync_analysis() -> None:
    record_id = "ACC-00018821"

    print("=" * 70)
    print("CRM-ERP Sync Boundary Demo — Salesforce ↔ SAP S/4HANA")
    print("=" * 70)
    print(f"\nRecord:       {record_id}  (Account / Business Partner)")
    print(f"Last sync:    {LAST_SYNC_TIME.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Analysis at:  {NOW.strftime('%Y-%m-%d %H:%M UTC')}")

    print("\n" + "─" * 70)
    print("FIELD AUTHORITY MAP")
    print("─" * 70)
    sf_fields = ACCOUNT_BOUNDARY.fields_owned_by(RecordAuthority.SYSTEM_A)
    sap_fields = ACCOUNT_BOUNDARY.fields_owned_by(RecordAuthority.SYSTEM_B)
    shared_fields = ACCOUNT_BOUNDARY.fields_owned_by(RecordAuthority.SHARED)

    print(f"\n  Salesforce-owned ({len(sf_fields)} fields):")
    for f in sf_fields:
        print(f"    • {f}")
    print(f"\n  SAP-owned ({len(sap_fields)} fields):")
    for f in sap_fields:
        print(f"    • {f}")
    print(f"\n  Shared / manual-review ({len(shared_fields)} fields):")
    for f in shared_fields:
        print(f"    • {f}")
    print("\n  Excluded (not synced):")
    for f in sorted(ACCOUNT_BOUNDARY.excluded_fields):
        print(f"    • {f}")

    print("\n" + "─" * 70)
    print("CHANGE DETECTION")
    print("─" * 70)

    all_fields = ACCOUNT_BOUNDARY.synced_fields()
    conflicts: list[tuple[SyncConflict, RecordAuthority | None]] = []
    clean_updates: list[str] = []
    no_change: list[str] = []

    for fname in sorted(all_fields):
        sf_val = SF_CURRENT.get(fname)
        sap_val = SAP_CURRENT.get(fname)
        last_val = LAST_SYNC_VALUES.get(fname)
        authority = ACCOUNT_BOUNDARY.authority_for(fname)

        sf_ts = SF_FIELD_TIMESTAMPS.get(fname)
        sap_ts = SAP_FIELD_TIMESTAMPS.get(fname)

        conflict = ACCOUNT_BOUNDARY.detect_conflict(
            field_name=fname,
            value_a=sf_val,
            value_b=sap_val,
            record_id=record_id,
            last_sync_value=last_val,
            system_a_modified_at=sf_ts,
            system_b_modified_at=sap_ts,
        )

        if conflict:
            conflicts.append((conflict, authority))
        elif sf_val != last_val or sap_val != last_val:
            clean_updates.append(fname)
        else:
            no_change.append(fname)

    print(f"\n  No change ({len(no_change)} fields):  {', '.join(sorted(no_change)) or '—'}")

    if clean_updates:
        print(f"\n  Clean updates ({len(clean_updates)} field(s)) — one system changed, apply directly:")
        for fname in clean_updates:
            sf_val = SF_CURRENT.get(fname)
            sap_val = SAP_CURRENT.get(fname)
            last_val = LAST_SYNC_VALUES.get(fname)
            authority = ACCOUNT_BOUNDARY.authority_for(fname)
            if sf_val != last_val:
                print(f"    • {fname}: '{last_val}' → '{sf_val}' (SF changed; authority={authority.value})")
            else:
                print(f"    • {fname}: '{last_val}' → '{sap_val}' (SAP changed; authority={authority.value})")

    print(f"\n  Conflicts ({len(conflicts)} field(s)) — BOTH systems changed since last sync:")
    for conflict, authority in conflicts:
        print(f"    • {conflict.field_name}")
        sf_ts = SF_FIELD_TIMESTAMPS.get(conflict.field_name, "unknown")
        sap_ts = SAP_FIELD_TIMESTAMPS.get(conflict.field_name, "unknown")
        print(f"        SF:       '{conflict.value_a}'  (modified {sf_ts})")
        print(f"        SAP:      '{conflict.value_b}'  (modified {sap_ts})")
        print(f"        Last sync: '{conflict.last_sync_value}'")
        print(f"        Authority: {authority.value if authority else 'shared'}")

    print("\n" + "─" * 70)
    print("CONFLICT RESOLUTION")
    print("─" * 70)

    decisions: list[SyncDecision] = []

    for conflict, authority in conflicts:
        fname = conflict.field_name

        if authority == RecordAuthority.SHARED:
            # Shared fields always escalate to manual review. Legal entity name
            # and VAT registration are legally binding — an automated last-writer-wins
            # rule risks silently applying an incorrect legal name. The cost of one
            # manual review is lower than the risk of a compliance error.
            decision = resolve_manual_review(conflict)
        elif authority in (RecordAuthority.SYSTEM_A, RecordAuthority.SYSTEM_B):
            # Owned fields: authority always wins
            decision = resolve_authority_wins(conflict, authority)
        else:
            decision = resolve_manual_review(conflict)

        decisions.append(decision)

    for d in decisions:
        print(f"\n  {d.field_name}")
        print(f"    Strategy:   {d.resolution_strategy}")
        if d.requires_manual_review:
            print("    Status:     ⛔  BLOCKED — requires human review")
            print(f"    Notes:      {d.notes}")
        else:
            print(f"    Apply:      '{d.winning_value}' (from {d.winning_system})")
            print(f"    Notes:      {d.notes}")

    print("\n" + "─" * 70)
    print("SYNC PLAN SUMMARY")
    print("─" * 70)

    apply_count = sum(1 for d in decisions if not d.requires_manual_review) + len(clean_updates)
    review_count = sum(1 for d in decisions if d.requires_manual_review)

    print(f"\n  Fields to apply:          {apply_count}")
    print(f"  Fields pending review:    {review_count}")
    print(f"  Fields unchanged:         {len(no_change)}")
    print()

    # Show the net sync operations
    print("  Operations to execute:")

    for fname in clean_updates:
        sf_val = SF_CURRENT.get(fname)
        sap_val = SAP_CURRENT.get(fname)
        last_val = LAST_SYNC_VALUES.get(fname)
        authority = ACCOUNT_BOUNDARY.authority_for(fname)
        if authority == RecordAuthority.SYSTEM_A and sf_val != last_val:
            print(f"    WRITE  SAP.{fname} = '{sf_val}' (source: SF)")
        elif authority == RecordAuthority.SYSTEM_B and sap_val != last_val:
            print(f"    WRITE  SF.{fname} = '{sap_val}' (source: SAP)")

    for d in decisions:
        if not d.requires_manual_review:
            other = "sap_s4" if d.winning_system == "salesforce" else "salesforce"
            system_prefix = "SAP" if other == "sap_s4" else "SF"
            print(f"    WRITE  {system_prefix}.{d.field_name} = '{d.winning_value}' (source: {d.winning_system})")

    for d in decisions:
        if d.requires_manual_review:
            print(f"    QUEUE  MANUAL_REVIEW({d.field_name}) → assign to data steward")

    print()
    print("  Fields that will NOT be synced (excluded from boundary):")
    for ef in sorted(ACCOUNT_BOUNDARY.excluded_fields):
        print(f"    SKIP   {ef}")

    print()
    print("─" * 70)
    print("KEY DESIGN POINTS")
    print("─" * 70)
    print("""
  1. Authority is field-level, not record-level.
     Different fields on the same account record have different owners.
     This matches real enterprise deployments — sales owns the segment,
     finance owns the credit limit.

  2. Conflict detection uses last-sync-value, not timestamps alone.
     If only one system changed a field since the last sync, it is a
     clean update — no conflict even if both systems show different values.
     Conflicts require both systems to have modified the field independently.

  3. Shared fields escalate to manual review.
     Legal entity name is sensitive and legally binding. When both CRM and
     ERP update it independently, a human data steward must resolve the
     discrepancy — automated last-writer-wins risks applying an incorrect
     legal name silently.

  4. Excluded fields are never touched.
     CRM lead score and SAP cost center are excluded from the boundary. The
     sync engine never reads or writes them. This prevents accidental
     cross-contamination of system-internal fields.

  5. allow_system_b_override=True.
     If SAP modifies a Salesforce-owned field (e.g., email), the change
     is detected and queued for review rather than silently discarded. This
     makes unexpected cross-system writes visible.
""")


if __name__ == "__main__":
    run_sync_analysis()
