"""Local synthetic workflow; no provider calls, network export, or real approvals."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from pathlib import Path

from confidence_escalation import ConfidenceEscalationMiddleware, ConfidenceScore, ScoringMethod
from enterprise_rag_patterns.compliance import FERPAContextPolicy, RecordCategory, StudentIdentityScope
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from regulated_ai_governance.agent_guard import GovernedActionGuard
from regulated_ai_governance.policy import ActionPolicy
from voice_ai_governance.pii import PIIScrubber
from voice_ai_governance.state import WarmTransferStateManager

from integration_automation_patterns.sqlite_outbox import SQLiteOutbox

CANARY = "UNAUTHORIZED_CANARY"


class LocalSpans(SpanExporter):
    """Small local sink retaining identifiers, operation names and numeric counts only."""

    def __init__(self, path):
        self.path = path

    def export(self, spans):
        with self.path.open("a") as stream:
            for span in spans:
                stream.write(
                    json.dumps(
                        {
                            "name": span.name,
                            "trace_id": format(span.context.trace_id, "032x"),
                            "span_id": format(span.context.span_id, "016x"),
                            "parent_id": format(span.parent.span_id, "016x") if span.parent else None,
                            "duration_ns": span.end_time - span.start_time,
                        }
                    )
                    + "\n"
                )
        return SpanExportResult.SUCCESS


def telemetry(directory):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(LocalSpans(directory / "spans.jsonl")))
    return provider, provider.get_tracer("synthetic-governed-service", "1.0")


def initialize(directory):
    directory.mkdir(parents=True, exist_ok=True)
    box = SQLiteOutbox(directory / "workflow.db")
    with box.connect() as db:
        db.executescript("""CREATE TABLE requests(id TEXT PRIMARY KEY, confidence REAL, allowed INTEGER,
            approved INTEGER DEFAULT 0, reviewer TEXT, status TEXT DEFAULT 'new');
            CREATE TABLE effects(id TEXT);
            CREATE TABLE audit(record TEXT);""")
    return box


def retrieve(directory, tracer):
    records = [
        {
            "student_id": "synthetic-a",
            "institution_id": "synthetic-campus",
            "record_category": RecordCategory.ACADEMIC_RECORD.value,
            "content": "Synthetic authorized course status",
        },
        {
            "student_id": "synthetic-b",
            "institution_id": "synthetic-campus",
            "record_category": RecordCategory.ACADEMIC_RECORD.value,
            "content": CANARY,
        },
        {"content": CANARY + "_missing_metadata"},
        {
            "student_id": "synthetic-a",
            "institution_id": "other-campus",
            "record_category": RecordCategory.ACADEMIC_RECORD.value,
            "content": CANARY + "_cross_campus",
        },
    ]
    policy = FERPAContextPolicy(
        StudentIdentityScope(
            "synthetic-a",
            "synthetic-campus",
            "fixture-principal",
            authorized_categories={RecordCategory.ACADEMIC_RECORD},
        ),
        audit_sink=lambda record: (directory / "retrieval_audit.log").write_text(record.to_log_entry()),
    )
    with tracer.start_as_current_span("retrieval.filter"):
        safe = policy.filter_retrieved_documents(records)
        policy.record_access([RecordCategory.ACADEMIC_RECORD], workflow_context="synthetic-demo")
    with tracer.start_as_current_span("model.fake_input"):
        # This is the exact content passed to the fake model, not a retrieval-count surrogate.
        model_input = [record["content"] for record in safe]
        (directory / "model_input.json").write_text(json.dumps(model_input))
        return model_input


def review_fixture(box, request_id, revoke=False):
    """Explicit synthetic reviewer fixture, not real human authorization."""
    with box.connect() as db:
        db.execute(
            "UPDATE requests SET approved=1, reviewer='synthetic-reviewer', allowed=? WHERE id=?",
            (0 if revoke else 1, request_id),
        )


def worker(directory, request_id, crash=False):
    box = SQLiteOutbox(directory / "workflow.db")
    provider, tracer = telemetry(directory)
    with box.connect() as db:
        row = db.execute(
            "SELECT confidence,allowed,approved,reviewer FROM requests WHERE id=?", (request_id,)
        ).fetchone()
    if row is None:
        raise ValueError("Unknown request")
    confidence, allowed, approved, reviewer = row
    score = ConfidenceScore(
        0.95 if approved and reviewer == "synthetic-reviewer" else confidence, ScoringMethod.COMPOSITE
    )

    def audit(record):
        with box.connect() as db:
            db.execute("INSERT INTO audit VALUES (?)", (json.dumps(record.to_log_entry()),))

    guard = GovernedActionGuard(
        ActionPolicy(
            allowed_actions={"create_service_case"}, denied_actions=set() if allowed else {"create_service_case"}
        ),
        audit_sink=audit,
        require_audit=True,
        audit_execution=True,
        raise_on_deny=True,
        policy_version="synthetic-current",
    )
    gate = ConfidenceEscalationMiddleware()

    def execute():
        box.enqueue(request_id, {"request_id": request_id})
        with tracer.start_as_current_span("outbox.consume"):
            box.consume(
                request_id,
                {"request_id": request_id},
                lambda db, payload: db.execute("INSERT INTO effects VALUES (?)", (payload["request_id"],)),
            )
        if crash:
            # Deliberately terminate a separate process after commit, before publication marking.
            provider.force_flush()
            os._exit(23)
        box.mark_published(request_id)

    try:
        with tracer.start_as_current_span("action.current_policy", context=extract(dict(os.environ))):
            guard.guard("create_service_case", lambda: gate.call_guarded(execute, score))
        status = "done"
    except PermissionError:
        status = "pending_review" if allowed else "denied"
    with box.connect() as db:
        db.execute("UPDATE requests SET status=? WHERE id=?", (status, request_id))
    provider.shutdown()


def run(directory):
    started = time.perf_counter()
    box = initialize(directory)
    provider, tracer = telemetry(directory)
    with tracer.start_as_current_span("synthetic.workflow"):
        model_input = retrieve(directory, tracer)
        carrier = {}
        inject(carrier)

        def invoke(request_id, crash=False):
            args = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                request_id,
                "--directory",
                str(directory),
            ]
            if crash:
                args.append("--crash")
            result = subprocess.run(args, env={**os.environ, **carrier}, capture_output=True, text=True, timeout=30)
            expected = 23 if crash else 0
            if result.returncode != expected:
                raise RuntimeError(f"Worker returned {result.returncode}: {result.stderr}")

        for request_id, confidence, allowed in [
            ("success", 0.95, 1),
            ("approved", 0.2, 1),
            ("revoked", 0.2, 1),
            ("denied", 0.95, 0),
        ]:
            with box.connect() as db:
                db.execute(
                    "INSERT INTO requests(id,confidence,allowed) VALUES (?,?,?)", (request_id, confidence, allowed)
                )
            invoke(request_id, crash=request_id == "success")
        with box.connect() as db:
            initial = dict(db.execute("SELECT id,status FROM requests"))
            effects_before_replay = db.execute("SELECT count(*) FROM effects WHERE id='success'").fetchone()[0]
        pending_after_crash = [event_id for event_id, _ in box.pending()]
        invoke("success")
        review_fixture(box, "approved")
        review_fixture(box, "revoked", revoke=True)
        invoke("approved")
        invoke("revoked")
        with tracer.start_as_current_span("voice.handoff"):
            voice = WarmTransferStateManager(pii_scrubber=PIIScrubber())
            sid = voice.create_session()
            voice.update_state(
                sid, lambda state: state.add_turn("user", "", entities_detected={"email": "fixture@example.com"})
            )
            handoff = asdict(voice.build_handoff_payload(sid, "synthetic-review"))
            (directory / "handoff.json").write_text(json.dumps(handoff))
    provider.shutdown()
    with box.connect() as db:
        effects = [row[0] for row in db.execute("SELECT id FROM effects")]
        statuses = dict(db.execute("SELECT id,status FROM requests"))
        audit_records = [json.loads(row[0]) for row in db.execute("SELECT record FROM audit")]
    spans = [json.loads(line) for line in (directory / "spans.jsonl").read_text().splitlines()]
    result = {
        "scope": "synthetic local fixtures; fake model and reviewer; not production calibration",
        "unauthorized_disclosures": sum(CANARY in item for item in model_input),
        "unauthorized_actions": sum(item in {"revoked", "denied"} for item in effects),
        "duplicate_effects": len(effects) - len(set(effects)),
        "business_effects": effects,
        "escalation_accuracy": sum(
            (initial[key] == "pending_review") == expected
            for key, expected in {"approved": True, "revoked": True, "denied": False, "success": False}.items()
        )
        / 4,
        "effects_before_replay": effects_before_replay,
        "pending_after_crash": pending_after_crash,
        "statuses": statuses,
        "handoff_redaction_failures": int("fixture@example.com" in json.dumps(handoff)),
        "audit_record_count": len(audit_records),
        "span_count": len(spans),
        "shared_trace_count": len({span["trace_id"] for span in spans}),
        "latency_ms": round((time.perf_counter() - started) * 1000, 3),
    }
    (directory / "report.json").write_text(json.dumps(result, indent=2))
    assert all(
        result[key] == 0
        for key in (
            "unauthorized_disclosures",
            "unauthorized_actions",
            "duplicate_effects",
            "handoff_redaction_failures",
        )
    )
    assert effects == ["success", "approved"]
    assert statuses["revoked"] == statuses["denied"] == "denied"
    assert effects_before_replay == 1 and pending_after_crash == ["success"]
    assert result["escalation_accuracy"] == 1 and result["shared_trace_count"] == 1
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--worker")
    parser.add_argument("--crash", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker(args.directory, args.worker, args.crash)
    else:
        directory = args.directory or Path(tempfile.mkdtemp(prefix="governed-service-"))
        print(json.dumps({"directory": str(directory), **run(directory)}, indent=2))
