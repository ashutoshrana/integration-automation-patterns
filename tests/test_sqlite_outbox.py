from concurrent.futures import ThreadPoolExecutor

import pytest

from integration_automation_patterns.sqlite_outbox import SQLiteOutbox


def test_crash_replay_two_workers_and_atomic_effect(tmp_path):
    path = tmp_path / "events.db"
    store = SQLiteOutbox(path)
    with store.connect() as db:
        db.execute("CREATE TABLE effects (id INTEGER PRIMARY KEY, value TEXT)")
    store.enqueue("evt1", {"value": "once"})

    def deliver(_):
        consumer = SQLiteOutbox(path)
        return consumer.consume(
            "evt1", {"value": "once"}, lambda db, p: db.execute("INSERT INTO effects(value) VALUES (?)", (p["value"],))
        )

    # Broker accepted the event, then publisher crashed BEFORE mark_published.
    assert deliver(None)
    restarted = SQLiteOutbox(path)
    assert len(restarted.pending()) == 1
    with ThreadPoolExecutor(2) as pool:
        assert list(pool.map(deliver, range(2))) == [False, False]
    restarted.mark_published("evt1")
    assert restarted.pending() == []
    with store.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 1
    with pytest.raises(ValueError, match="different payload"):
        store.consume("evt1", {"value": "changed"}, lambda db, p: None)


def test_transaction_rollback_and_simultaneous_first_delivery(tmp_path):
    store = SQLiteOutbox(tmp_path / "events.db")
    with store.connect() as db:
        db.execute("CREATE TABLE effects(value TEXT)")

    def failed_write(db):
        db.execute("INSERT INTO effects VALUES ('rollback')")
        raise RuntimeError("crash")

    with pytest.raises(RuntimeError):
        store.enqueue("fail", {}, failed_write)
    assert store.pending() == []

    def effect(db, payload):
        db.execute("INSERT INTO effects VALUES ('once')")

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda _: store.consume("same", {}, effect), range(2)))
    assert sorted(results) == [False, True]
    with store.connect() as db:
        assert db.execute("SELECT value FROM effects").fetchall() == [("once",)]

    def failing_effect(db, payload):
        effect(db, payload)
        raise RuntimeError("crash")

    with pytest.raises(RuntimeError):
        store.consume("retry", {}, failing_effect)
    assert store.consume("retry", {}, effect)
