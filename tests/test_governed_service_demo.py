"""Opt-in sibling-library demo integration test (see docs/GOVERNED_SERVICE_DEMO.md)."""

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

for dependency in (
    "enterprise_rag_patterns",
    "regulated_ai_governance",
    "confidence_escalation",
    "voice_ai_governance",
    "opentelemetry.sdk",
):
    pytest.importorskip(dependency)

DEMO = Path(__file__).resolve().parents[1] / "examples" / "governed_service_demo.py"
spec = importlib.util.spec_from_file_location("governed_service_demo", DEMO)
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


def test_governed_workflow_survives_restart_without_policy_bypass(tmp_path):
    result = demo.run(tmp_path)
    assert result["unauthorized_disclosures"] == 0
    assert json.loads((tmp_path / "model_input.json").read_text()) == ["Synthetic authorized course status"]
    assert (tmp_path / "retrieval_audit.log").read_text()
    assert result["unauthorized_actions"] == result["duplicate_effects"] == 0
    assert result["effects_before_replay"] == 1
    assert result["pending_after_crash"] == ["success"]
    assert result["business_effects"] == ["success", "approved"]
    assert result["statuses"] == {"success": "done", "approved": "done", "revoked": "denied", "denied": "denied"}
    assert result["escalation_accuracy"] == 1
    assert result["handoff_redaction_failures"] == 0
    assert result["shared_trace_count"] == 1
    assert result["latency_ms"] > 0
    spans = (tmp_path / "spans.jsonl").read_text()
    assert demo.CANARY not in spans and "fixture@example.com" not in spans
    assert "Synthetic authorized course status" not in spans
    with sqlite3.connect(tmp_path / "workflow.db") as db:
        audits = [json.loads(row[0]) for row in db.execute("SELECT record FROM audit")]
        assert db.execute("SELECT count(*) FROM outbox WHERE published=0").fetchone()[0] == 0
        assert db.execute("SELECT reviewer FROM requests WHERE id='approved'").fetchone()[0] == "synthetic-reviewer"
    assert any(not audit["permitted"] for audit in audits)
    assert any(audit["outcome"] == "succeeded" for audit in audits)
