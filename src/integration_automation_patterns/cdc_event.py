"""
cdc_event.py — Change Data Capture (CDC) event envelope.

Provides a typed CDC event structure for processing database change streams
from Debezium, AWS DMS, GCP Datastream, or Azure Data Factory.

CDC events represent row-level changes (INSERT/UPDATE/DELETE) in a source
system and are used to propagate changes to downstream systems (search indexes,
analytics warehouses, read-model projections).

Usage::

    # Deserialise from a Kafka topic (Debezium format)
    raw = consumer.poll()
    event = CDCEvent.from_debezium(raw.value)

    match event.operation:
        case CDCOperation.INSERT:
            search_index.upsert(event.after)
        case CDCOperation.UPDATE:
            search_index.upsert(event.after)
            audit_log.record_change(event)
        case CDCOperation.DELETE:
            search_index.delete(event.before["id"])
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class CDCOperation(Enum):
    """Type of database change captured."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SNAPSHOT = "SNAPSHOT"  # Initial load snapshot record
    TRUNCATE = "TRUNCATE"


@dataclass
class CDCSourceMetadata:
    """
    Source-system context attached to a CDC event.

    Attributes:
        database: Source database name.
        schema: Source schema (e.g. ``"public"``).
        table: Source table name.
        connector: CDC connector type (``"debezium"``, ``"dms"``, ``"datastream"``).
        lsn: Log Sequence Number or equivalent offset, or ``None``.
        server_id: Source server/shard identifier, or ``None``.
    """

    database: str
    schema: str
    table: str
    connector: str = "unknown"
    lsn: str | None = None
    server_id: str | None = None


@dataclass
class CDCEvent:
    """
    A single change data capture event from a source database.

    Attributes:
        event_id: UUID for this CDC event (idempotency key for downstream processing).
        operation: Type of change (INSERT/UPDATE/DELETE/SNAPSHOT).
        source: Source system metadata.
        before: Row state before the change, or ``None`` for INSERTs.
        after: Row state after the change, or ``None`` for DELETEs.
        captured_at: UTC timestamp when the change was captured.
        transaction_id: Source transaction ID if available.
    """

    operation: CDCOperation
    source: CDCSourceMetadata
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    transaction_id: str | None = None

    @classmethod
    def from_debezium(cls, payload: dict[str, Any]) -> CDCEvent:
        """
        Deserialise a Debezium CDC event payload.

        Expects the standard Debezium envelope format with ``op``, ``before``,
        ``after``, and ``source`` keys.

        Args:
            payload: Parsed Debezium JSON payload dict.

        Returns:
            A ``CDCEvent`` instance.

        Raises:
            KeyError: If required Debezium fields are missing.
            ValueError: If ``op`` contains an unrecognised operation code.
        """
        op_map = {
            "c": CDCOperation.INSERT,
            "u": CDCOperation.UPDATE,
            "d": CDCOperation.DELETE,
            "r": CDCOperation.SNAPSHOT,
            "t": CDCOperation.TRUNCATE,
        }
        op_code = payload.get("op", "")
        operation = op_map.get(op_code)
        if operation is None:
            raise ValueError(f"Unrecognised Debezium operation code: '{op_code}'")

        src = payload.get("source", {})
        source = CDCSourceMetadata(
            database=src.get("db", ""),
            schema=src.get("schema", ""),
            table=src.get("table", ""),
            connector="debezium",
            lsn=str(src.get("lsn")) if src.get("lsn") is not None else None,
            server_id=str(src.get("server_id")) if src.get("server_id") is not None else None,
        )

        ts_ms = src.get("ts_ms")
        captured_at = (
            datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms is not None else datetime.now(timezone.utc)
        )

        return cls(
            operation=operation,
            source=source,
            before=payload.get("before"),
            after=payload.get("after"),
            captured_at=captured_at,
            transaction_id=str(payload.get("transaction", {}).get("id", "")) or None,
        )

    def changed_fields(self) -> set[str]:
        """
        Return the set of field names that changed between ``before`` and ``after``.

        Only meaningful for UPDATE operations.  Returns an empty set for
        INSERT, DELETE, or SNAPSHOT operations.
        """
        if self.operation != CDCOperation.UPDATE or self.before is None or self.after is None:
            return set()
        return {k for k in self.after if self.after.get(k) != self.before.get(k)}

    def to_audit_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serialisable dict for audit logging.

        Excludes full before/after payloads to avoid logging sensitive data —
        includes only the set of changed fields.
        """
        return {
            "event_id": self.event_id,
            "operation": self.operation.value,
            "table": f"{self.source.schema}.{self.source.table}",
            "database": self.source.database,
            "connector": self.source.connector,
            "captured_at": self.captured_at.isoformat(),
            "transaction_id": self.transaction_id,
            "changed_fields": sorted(self.changed_fields()),
            "lsn": self.source.lsn,
        }
