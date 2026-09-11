"""Durable at-least-once outbox and atomic SQLite consumer deduplication.

Business callbacks must write ONLY through the supplied connection. External
side effects cannot be made atomic by this database transaction.
"""

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class SQLiteOutbox:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("durable file path required")
        with self.connect() as db:
            db.executescript(
                "CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                "published INTEGER NOT NULL DEFAULT 0);"
                "CREATE TABLE IF NOT EXISTS inbox (id TEXT PRIMARY KEY, payload TEXT NOT NULL);"
            )

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def enqueue(
        self,
        event_id: str,
        payload: dict[str, Any],
        business_write: Callable[[sqlite3.Connection], None] | None = None,
    ) -> bool:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if not event_id:
            raise ValueError("event_id is required")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT payload FROM outbox WHERE id=?", (event_id,)).fetchone()
            if previous:
                if previous[0] != encoded:
                    raise ValueError("event_id reused with different payload")
                return False
            db.execute("INSERT INTO outbox(id,payload) VALUES (?,?)", (event_id, encoded))
            if business_write:
                business_write(db)
        return True

    def pending(self) -> list[tuple[str, dict[str, Any]]]:
        with self.connect() as db:
            return [
                (row[0], json.loads(row[1]))
                for row in db.execute("SELECT id,payload FROM outbox WHERE published=0 ORDER BY rowid")
            ]

    def mark_published(self, event_id: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE outbox SET published=1 WHERE id=?", (event_id,))

    def consume(
        self,
        event_id: str,
        payload: dict[str, Any],
        effect: Callable[[sqlite3.Connection, dict[str, Any]], None],
    ) -> bool:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if not event_id:
            raise ValueError("event_id is required")
        # ponytail: SQLite serializes writers; use a transactional server DB for higher throughput.
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT payload FROM inbox WHERE id=?", (event_id,)).fetchone()
            if previous:
                if previous[0] != encoded:
                    raise ValueError("event_id reused with different payload")
                return False
            db.execute("INSERT INTO inbox VALUES (?,?)", (event_id, encoded))
            effect(db, payload)
        return True
