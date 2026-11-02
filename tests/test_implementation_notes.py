"""Execute documented API examples and resolve links in the implementation notes."""

from __future__ import annotations

import ast
import re
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = [*sorted((ROOT / "docs").glob("implementation-note-*.md")), ROOT / "docs/agentic-security-trends-2026.md"]


def blocks(name):
    return re.findall(r"```python\n(.*?)```", (ROOT / "docs" / name).read_text(), re.S)


def test_envelope_note_executes_with_documented_retry_boundary():
    namespace = {}
    for block in blocks("implementation-note-02.md"):
        exec(block, namespace)
    event = namespace["envelope"]
    event.mark_dispatched()
    event.mark_failed()
    assert event.next_retry_wait_seconds() == 120
    event.mark_dispatched()
    event.mark_failed()
    assert event.is_terminal()
    assert event.status.value == "failed"


def test_sync_boundary_constructor_and_approval_deadline_examples():
    namespace = {}
    exec(blocks("implementation-note-03.md")[0], namespace)
    boundary = namespace["ACCOUNT_BOUNDARY"]
    assert not boundary.is_synced("sf_lead_score")
    assert boundary.detect_conflict("payment_terms", "NET30", "NET45", "a1", "NET30") is None
    approval = runpy.run_path(str(ROOT / "examples/08_approval_workflow.py"))
    for block in blocks("implementation-note-06.md"):
        exec(block, approval)
    assert approval["gate"].escalation_fn().approver_id == "general_counsel"


@pytest.mark.parametrize("path", DOCS, ids=lambda p: p.name)
def test_local_document_links_and_heading_fragments(path):
    text = path.read_text()
    for target in re.findall(r"\]\(([^)]+)\)", text):
        if re.match(r"[a-z]+://", target):
            continue
        filename, _, fragment = target.partition("#")
        resolved = (path.parent / filename).resolve() if filename else path
        assert resolved.is_file(), (path.name, target)
        if fragment:
            body = re.sub(r"```.*?```", "", resolved.read_text(), flags=re.S)
            headings = re.findall(r"^#{1,6}\s+(.+)$", body, re.M)
            anchors = {re.sub(r"[^\w\- ]", "", h.lower()).replace(" ", "-") for h in headings}
            assert fragment in anchors, (path.name, target)


def test_security_reference_classes_exist_in_named_examples():
    text = (ROOT / "docs/agentic-security-trends-2026.md").read_text()
    rows = re.findall(r"^\|[^\n]+\| `(examples/[^`]+\.py)` \| ([^\n]+)\|$", text, re.M)
    assert rows
    for filename, cell in rows:
        tree = ast.parse((ROOT / filename).read_text())
        classes = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
        assert set(re.findall(r"`(\w+)`", cell)) <= classes, filename


def test_cqrs_note_rejects_a_stale_version():
    namespace = runpy.run_path(str(ROOT / "examples/07_cqrs_event_sourcing.py"))
    store = namespace["EventStore"]()
    make_event = namespace["_make_event"]
    for version in range(4):
        store.append("opp", [make_event("stage", "opp", version, {})], version - 1)
    namespace.update(
        store=store, opp_id="opp", reconstruct=lambda events: events, other_event=make_event("stage", "opp", 4, {})
    )
    with pytest.raises(namespace["ConcurrencyConflictError"]):
        exec(blocks("implementation-note-05.md")[1], namespace)
    assert store.current_version("opp") == 4


def test_documented_replay_wiring_uses_dedicated_store(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace

    monkeypatch.chdir(tmp_path)
    namespace = {"outbox": SimpleNamespace(get_pending=lambda **kwargs: []), "publisher": lambda event: True}
    exec(blocks("implementation-note-04.md")[-1], namespace)
    result = asyncio.run(namespace["engine"].replay())
    assert result.failed == 0
    assert (tmp_path / "replay-markers.sqlite3").is_file()
