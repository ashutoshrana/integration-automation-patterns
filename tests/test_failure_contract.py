"""Executable failure guarantees; timings are observations, never CI thresholds."""

import importlib.util
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "failure_contract", Path(__file__).resolve().parents[1] / "examples" / "failure_contract.py"
)
assert SPEC and SPEC.loader
harness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(harness)


def test_failure_contract_processes_http_and_baseline(tmp_path):
    result = harness.run(tmp_path / "run", events=8, workers=(1, 2), timeout=30)
    before, after = result["crashes"]
    assert before["effects_after_crash"] == 0
    assert before["replay_created"] is True
    assert after["effects_after_crash"] == 1
    assert after["replay_created"] is False
    assert all(row["final_effects"] == row["pending_after_crash"] == 1 for row in result["crashes"])
    negative, protected = result["http"]
    assert negative["remote_effects"] == 2
    assert protected["remote_effects"] == 1
    assert all(row["client_outcomes"] == ["unknown", "acknowledged"] for row in result["http"])
    assert all(row["requests"] == 2 and row["pending"] == 0 for row in result["http"])
    settings = []
    for row in result["benchmarks"]:
        assert row["effects"] == row["created"] == row["unique_events"] == 8
        assert row["sqlite_errors"] == 0
        assert row["attempts"] == 16 * row["workers"]
        assert row["latency_ms"]["p99"] >= row["latency_ms"]["p50"] >= 0
        settings.append(row["sqlite_settings"])
    assert all(setting == settings[0] for setting in settings)
    assert result["lock"]["completed_while_locked"] is False
    assert result["lock"]["created"] is True
    contention = result["lock"]["observed_contention"]
    assert contention["confirmed"] is True
    if sys.version_info >= (3, 11):
        assert contention["code"] in (5, 6)
        assert contention["name"] in ("SQLITE_BUSY", "SQLITE_LOCKED")
    else:
        assert contention["evidence"] == "sqlite_exception_message"
        assert contention["message"] in ("database is locked", "database table is locked")
    assert result["backlog"]["records"] == 8
    assert result["backlog"]["python_peak_bytes"] > 0


@pytest.mark.parametrize("engine", ["helper", "sql"])
def test_baseline_preserves_payload_identity_and_rolls_back_bad_effect(tmp_path, engine):
    path = tmp_path / "events.sqlite"
    store = harness.initialize(path)

    def consume(event_id, payload):
        if engine == "helper":
            return store.consume(event_id, payload, harness.effect)
        return harness.direct_consume(path, event_id, payload)

    assert consume("same", {"value": 1}) is True
    assert consume("same", {"value": 1}) is False
    with pytest.raises(ValueError):
        consume("same", {"value": 2})
    with pytest.raises(KeyError):
        consume("bad", {})
    assert harness.count(path, "effects") == harness.count(path, "inbox") == 1


@pytest.mark.parametrize(
    "kwargs", [{"events": 0}, {"events": 10001}, {"workers": (1, 1)}, {"workers": (9,)}, {"timeout": 0}]
)
def test_invalid_workload_does_not_create_directory(tmp_path, kwargs):
    directory = tmp_path / "untouched"
    with pytest.raises(AssertionError):
        harness.run(directory, **kwargs)
    assert not directory.exists()


def test_existing_directory_is_not_overwritten(tmp_path):
    marker = tmp_path / "keep"
    marker.write_text("preserved")
    with pytest.raises(FileExistsError):
        harness.run(tmp_path)
    assert marker.read_text() == "preserved"


def test_deadline_failure_kills_and_reaps_child():
    with pytest.raises(subprocess.TimeoutExpired), harness.children() as children:
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        children.append(process)
        process.wait(timeout=0.02)
    assert process.poll() is not None


def test_provenance_separates_stale_metadata_and_executed_source(monkeypatch):
    monkeypatch.setattr(harness.importlib.metadata, "version", lambda _: "0.0.0-stale")
    result = harness.provenance()
    assert result["installed_distribution_version"] == "0.0.0-stale"
    assert result["runtime_version"] == harness.integration_automation_patterns.__version__
    assert result["version_matches_metadata"] is False
    assert set(result["executed_source_sha256"]) == {"harness", "package_init", "sqlite_outbox"}
    assert all(len(value) == 64 for value in result["executed_source_sha256"].values())
    assert str(Path.home()) not in str(result)


def test_contention_requires_actual_sqlite_error():
    with pytest.raises(AssertionError, match="did not confirm"):
        harness.contention_evidence(sqlite3.OperationalError("unable to open database file"))
    evidence = harness.contention_evidence(sqlite3.OperationalError("database is locked"))
    assert evidence["confirmed"] is True
    assert evidence["code"] is None
    assert evidence["evidence"] == "sqlite_exception_message"
