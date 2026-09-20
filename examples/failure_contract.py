"""Local failure-contract experiment; stdlib only, with no remote service calls."""

import argparse
import contextlib
import hashlib
import http.client
import importlib
import importlib.metadata
import inspect
import json
import math
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import integration_automation_patterns
from integration_automation_patterns import SQLiteOutbox


def provenance():
    """Identify executed bytes separately from potentially stale installed metadata."""
    files = {
        "harness": Path(__file__).resolve(),
        "package_init": Path(integration_automation_patterns.__file__).resolve(),
        "sqlite_outbox": Path(inspect.getfile(SQLiteOutbox)).resolve(),
    }
    try:
        installed = importlib.metadata.version("integration-automation-patterns")
    except importlib.metadata.PackageNotFoundError:
        installed = None
    root = Path(__file__).resolve().parents[1]
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True, timeout=5
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"], cwd=root, check=True, capture_output=True, text=True, timeout=5
            ).stdout.strip()
        )
    except (OSError, subprocess.SubprocessError):
        revision, dirty = None, None
    return {
        "runtime_version": integration_automation_patterns.__version__,
        "installed_distribution_version": installed,
        "version_matches_metadata": installed == integration_automation_patterns.__version__,
        "executed_source_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in files.items()},
        "harness_checkout_revision": revision,
        "harness_checkout_dirty": dirty,
    }


def require(condition, message):
    if not condition:
        raise AssertionError(message)


@contextlib.contextmanager
def connection(path):
    db = sqlite3.connect(path, timeout=30)
    try:
        with db:
            yield db
    finally:
        db.close()


def initialize(path):
    store = SQLiteOutbox(path)
    with store.connect() as db:
        db.execute("CREATE TABLE effects(value INTEGER)")  # Deliberately NOT unique.
    return store


def effect(db, payload):
    db.execute("INSERT INTO effects VALUES (?)", (payload["value"],))


def direct_consume(path, event_id, payload):
    """Same serialization, connection and transaction contract as SQLiteOutbox.consume."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if not event_id:
        raise ValueError("event_id is required")
    with connection(path) as db:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute("SELECT payload FROM inbox WHERE id=?", (event_id,)).fetchone()
        if previous:
            if previous[0] != encoded:
                raise ValueError("event_id reused with different payload")
            return False
        db.execute("INSERT INTO inbox VALUES (?,?)", (event_id, encoded))
        effect(db, payload)
    return True


def count(path, table):
    require(table in {"effects", "inbox", "outbox"}, "Unexpected fixture table")
    with connection(path) as db:
        return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def wait_for(predicate, deadline, description):
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError(description)
        time.sleep(0.01)


@contextlib.contextmanager
def children():
    processes = []
    try:
        yield processes
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
        for process in processes:
            process.wait(timeout=5)


def launch(processes, directory, mode, events, timeout, index=0, engine="helper"):
    result = directory / f"result-{index}.json"
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            mode,
            "--directory",
            str(directory),
            "--events",
            str(events),
            "--timeout",
            str(timeout),
            "--index",
            str(index),
            "--engine",
            engine,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    processes.append(process)
    return process, result


def finish(process, deadline, expected=0):
    process.wait(timeout=max(0.01, deadline - time.monotonic()))
    require(process.returncode == expected, f"Worker exited {process.returncode}, expected {expected}")


def worker(args):
    directory = args.directory
    path = directory / "events.sqlite"
    store = SQLiteOutbox(path)
    ready = directory / f"ready-{args.index}"
    result = directory / f"result-{args.index}.json"
    if args.worker in {"benchmark", "lock"}:
        ready.touch()
        wait_for((directory / "start").exists, time.monotonic() + args.timeout, "Worker start deadline")
    if args.worker == "benchmark":
        samples, created, errors = [], 0, 0
        for _ in range(2):  # Every worker repeats the same IDs, including concurrent duplicates.
            for number in range(args.events):
                started = time.perf_counter_ns()
                try:
                    payload = {"value": number}
                    created += (
                        store.consume(str(number), payload, effect)
                        if args.engine == "helper"
                        else direct_consume(path, str(number), payload)
                    )
                except sqlite3.OperationalError:
                    errors += 1
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
        result.write_text(json.dumps({"samples_ms": samples, "created": created, "sqlite_errors": errors}))
    elif args.worker == "lock":
        started = time.perf_counter()
        probe = sqlite3.connect(path, timeout=0)
        try:
            try:
                probe.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                busy = contention_evidence(exc)
            else:
                raise AssertionError("Probe acquired a supposedly held lock")
        finally:
            probe.close()
        temporary = directory / "contention.tmp"
        temporary.write_text(json.dumps(busy))
        temporary.replace(directory / "contention.json")
        created = store.consume("locked", {"value": 1}, effect)
        result.write_text(json.dumps({"created": created, "wait_ms": (time.perf_counter() - started) * 1000}))
    else:

        def crash_effect(db, payload):
            effect(db, payload)
            os._exit(23)

        created = store.consume("crash", {"value": 1}, crash_effect if args.worker == "before" else effect)
        if args.worker == "after":
            os._exit(23)
        store.mark_published("crash")
        result.write_text(json.dumps({"created": created}))


def crash_case(directory, boundary, timeout):
    directory.mkdir()
    path = directory / "events.sqlite"
    store = initialize(path)
    store.enqueue("crash", {"value": 1})
    with children() as processes:
        process, _ = launch(processes, directory, boundary, 1, timeout)
        finish(process, time.monotonic() + timeout, expected=23)
        effects_after_crash = count(path, "effects")
        pending_after_crash = len(store.pending())
        require(effects_after_crash == int(boundary == "after"), "Wrong crash transaction boundary")
        require(pending_after_crash == 1, "Unacknowledged event disappeared")
        process, result = launch(processes, directory, "replay", 1, timeout)
        finish(process, time.monotonic() + timeout)
        replay = json.loads(result.read_text())
    require(count(path, "effects") == count(path, "inbox") == 1, "Crash recovery lost/duplicated effect")
    require(store.pending() == [], "Recovered event remains pending")
    return {
        "boundary": boundary,
        "exit_code": 23,
        "effects_after_crash": effects_after_crash,
        "pending_after_crash": pending_after_crash,
        "replay_created": replay["created"],
        "final_effects": 1,
    }


def http_case(directory, idempotent):
    directory.mkdir()
    remote = directory / "remote.sqlite"
    with connection(remote) as db:
        db.executescript("CREATE TABLE effects(value INTEGER); CREATE TABLE seen(id TEXT PRIMARY KEY,payload TEXT);")
    requests = 0

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(3)

        def log_message(self, *args):
            pass

        def do_POST(self):
            nonlocal requests
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                self.send_error(400)
                return
            body = self.rfile.read(length).decode()
            payload = json.loads(body)
            key = self.headers.get("Idempotency-Key")
            with connection(remote) as db:
                db.execute("BEGIN IMMEDIATE")
                previous = db.execute("SELECT payload FROM seen WHERE id=?", (key,)).fetchone() if key else None
                if previous and previous[0] != body:
                    self.send_error(409)
                    return
                if not previous:
                    effect(db, payload)
                    if key:
                        db.execute("INSERT INTO seen VALUES (?,?)", (key, body))
            requests += 1
            if requests == 1:  # Commit is durable BEFORE intentionally losing its acknowledgment.
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

    store = initialize(directory / "events.sqlite")
    store.enqueue("remote-effect", {"value": 1})
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        outcomes = []
        for _ in range(2):
            event_id, payload = store.pending()[0]
            client = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
            try:
                headers = {"Idempotency-Key": event_id} if idempotent else {}
                client.request("POST", "/effect", json.dumps(payload), headers)
                response = client.getresponse()
                response.read()
                require(response.status == 200, "Unexpected HTTP status")
            except (http.client.RemoteDisconnected, ConnectionResetError, TimeoutError):
                outcomes.append("unknown")
            else:
                store.mark_published(event_id)
                outcomes.append("acknowledged")
            finally:
                client.close()
        effects = count(remote, "effects")
        require(outcomes == ["unknown", "acknowledged"], "Lost response fixture did not execute")
        require(effects == (1 if idempotent else 2), "Wrong external idempotency outcome")
        require(store.pending() == [], "Acknowledged relay event remains pending")
        return {
            "downstream_idempotency": idempotent,
            "client_outcomes": outcomes,
            "remote_effects": effects,
            "requests": requests,
            "pending": 0,
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        require(not thread.is_alive(), "HTTP fixture did not stop")


def benchmark(directory, engine, events, workers, timeout):
    directory.mkdir()
    path = directory / "events.sqlite"
    initialize(path)
    with connection(path) as db:
        settings = {key: db.execute(f"PRAGMA {key}").fetchone()[0] for key in ("journal_mode", "synchronous")}
    deadline = time.monotonic() + timeout
    with children() as processes:
        jobs = [launch(processes, directory, "benchmark", events, timeout, i, engine) for i in range(workers)]
        wait_for(lambda: all((directory / f"ready-{i}").exists() for i in range(workers)), deadline, "Readiness")
        started = time.perf_counter()
        (directory / "start").touch()
        for process, _ in jobs:
            finish(process, deadline)
        elapsed = time.perf_counter() - started
        results = [json.loads(result.read_text()) for _, result in jobs]
    samples = sorted(sample for result in results for sample in result["samples_ms"])
    created = sum(result["created"] for result in results)
    errors = sum(result["sqlite_errors"] for result in results)
    effects = count(path, "effects")
    require(errors == 0, "SQLite operational errors occurred; this run is not a passing envelope")
    require(created == effects == count(path, "inbox") == events, "Benchmark safety invariant failed")
    return {
        "engine": engine,
        "workers": workers,
        "unique_events": events,
        "attempts": len(samples),
        "created": created,
        "effects": effects,
        "sqlite_errors": errors,
        "elapsed_seconds": elapsed,
        "attempts_per_second": len(samples) / elapsed,
        "latency_ms": {f"p{p}": samples[max(0, math.ceil(len(samples) * p / 100) - 1)] for p in (50, 95, 99)},
        "sqlite_settings": settings,
        "database_bytes": path.stat().st_size,
    }


def contention_evidence(exc):
    # sqlite_errorcode/name were added in Python 3.11; keep the 3.10 evidence explicit.
    code = getattr(exc, "sqlite_errorcode", None)
    message = str(exc)
    confirmed = code in (5, 6) if code is not None else message in ("database is locked", "database table is locked")
    require(confirmed, "Probe did not confirm SQLite BUSY/LOCKED contention")
    return {
        "confirmed": True,
        "code": code,
        "name": getattr(exc, "sqlite_errorname", None),
        "message": message,
        "evidence": "sqlite_errorcode" if code is not None else "sqlite_exception_message",
    }


def lock_case(directory, timeout):
    directory.mkdir()
    path = directory / "events.sqlite"
    initialize(path)
    deadline = time.monotonic() + timeout
    with children() as processes:
        process, result = launch(processes, directory, "lock", 1, timeout)
        wait_for((directory / "ready-0").exists, deadline, "Lock worker readiness")
        with connection(path) as db:
            db.execute("BEGIN IMMEDIATE")
            (directory / "start").touch()
            wait_for((directory / "contention.json").exists, deadline, "Confirmed SQLite contention")
            busy = json.loads((directory / "contention.json").read_text())
            require(busy["confirmed"] is True, "Missing confirmed contention")
            completed_while_locked = result.exists()
        finish(process, deadline)
        observed = json.loads(result.read_text())
    require(not completed_while_locked and observed["created"], "Lock transaction isolation failed")
    return {
        **observed,
        "observed_contention": busy,
        "completed_while_locked": completed_while_locked,
        "configured_timeout_seconds": 30,
    }


def backlog_case(directory, events):
    directory.mkdir()
    path = directory / "events.sqlite"
    store = initialize(path)
    # Bulk fixture setup is not part of a throughput measurement.
    with store.connect() as db:
        db.executemany(
            "INSERT INTO outbox(id,payload) VALUES (?,?)", ((str(i), json.dumps({"value": i})) for i in range(events))
        )
    tracemalloc.start()
    try:
        started = time.perf_counter()
        pending = store.pending()
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    require(len(pending) == events, "Backlog count mismatch")
    return {
        "records": len(pending),
        "elapsed_seconds": elapsed,
        "python_peak_bytes": peak,
        "database_bytes": path.stat().st_size,
        "measurement": "tracemalloc allocations, not process RSS",
    }


def run(directory, events=100, workers=(1, 2), timeout=120):
    require(1 <= events <= 10000, "events must be 1..10000")
    require(workers and all(1 <= n <= 8 for n in workers), "workers must be 1..8")
    require(len(set(workers)) == len(workers), "worker counts must be unique")
    require(0 < timeout <= 1800, "timeout must be 0..1800 seconds")
    directory.mkdir(parents=True, exist_ok=False)
    timings = []
    for n in workers:
        for engine in ("sql", "helper"):
            timings.append(benchmark(directory / f"{engine}-{n}", engine, events, n, timeout))
    return {
        "scope": "local synthetic observations, not production SLOs or remote exactly-once guarantees",
        "provenance": provenance(),
        "runtime": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
        },
        "events": events,
        "workers": list(workers),
        "deadline_per_process_group_seconds": timeout,
        "crashes": [crash_case(directory / f"crash-{b}", b, timeout) for b in ("before", "after")],
        "http": [http_case(directory / f"http-{mode}", mode) for mode in (False, True)],
        "benchmarks": timings,
        "lock": lock_case(directory / "lock", timeout),
        "backlog": backlog_case(directory / "backlog", events),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, help="New output directory; existing paths are never overwritten")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2])
    parser.add_argument("--timeout", type=float, default=120, help="Deadline per subprocess group (max 1800s)")
    parser.add_argument("--worker", choices=["benchmark", "lock", "before", "after", "replay"], help=argparse.SUPPRESS)
    parser.add_argument("--index", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--engine", choices=["sql", "helper"], default="helper", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args)
    elif args.directory:
        print(json.dumps(run(args.directory, args.events, args.workers, args.timeout), indent=2))
    else:
        with tempfile.TemporaryDirectory(prefix="failure-contract-") as temporary:
            print(json.dumps(run(Path(temporary) / "run", args.events, args.workers, args.timeout), indent=2))


if __name__ == "__main__":
    main()
