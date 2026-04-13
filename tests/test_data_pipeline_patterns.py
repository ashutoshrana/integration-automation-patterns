"""
Tests for 20_data_pipeline_patterns.py — ETL pipeline, checkpointing,
data quality validation, quality reports, and pipeline metrics.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_MOD_PATH = (
    Path(__file__).parent.parent / "examples" / "20_data_pipeline_patterns.py"
)


def _load_module():
    module_name = "data_pipeline_patterns"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def m():
    return _load_module()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _record(m, record_id, payload):
    return m.DataRecord(record_id=record_id, payload=payload)


# ===========================================================================
# TestCheckpointStore
# ===========================================================================


class TestCheckpointStore:
    """Tests for CheckpointStore: stage status and processed-record tracking."""

    def test_stage_initially_none(self, m):
        """No checkpoint saved → get_stage_status returns None."""
        store = m.CheckpointStore()
        assert store.get_stage_status("run-1", "stage-a") is None

    def test_save_and_retrieve_stage_status(self, m):
        """save_stage COMPLETED → get_stage_status returns COMPLETED."""
        store = m.CheckpointStore()
        store.save_stage("run-1", "stage-a", m.StageStatus.COMPLETED)
        assert store.get_stage_status("run-1", "stage-a") == m.StageStatus.COMPLETED

    def test_mark_and_check_processed(self, m):
        """mark_processed(run_id, [ids]) → is_processed returns True for each id."""
        store = m.CheckpointStore()
        store.mark_processed("run-1", ["rec-001", "rec-002"])
        assert store.is_processed("run-1", "rec-001") is True
        assert store.is_processed("run-1", "rec-002") is True
        assert store.is_processed("run-1", "rec-003") is False

    def test_unprocessed_records_filtered(self, m):
        """get_unprocessed returns only non-processed records."""
        store = m.CheckpointStore()
        records = [
            _record(m, "rec-001", "a"),
            _record(m, "rec-002", "b"),
            _record(m, "rec-003", "c"),
        ]
        store.mark_processed("run-1", ["rec-001", "rec-003"])
        result = store.get_unprocessed("run-1", records)
        assert len(result) == 1
        assert result[0].record_id == "rec-002"

    def test_clear_run_removes_data(self, m):
        """clear_run → stage status is None and is_processed is False."""
        store = m.CheckpointStore()
        store.save_stage("run-1", "stage-a", m.StageStatus.COMPLETED)
        store.mark_processed("run-1", ["rec-001"])
        store.clear_run("run-1")
        assert store.get_stage_status("run-1", "stage-a") is None
        assert store.is_processed("run-1", "rec-001") is False

    def test_completed_stages_list(self, m):
        """save 2 COMPLETED + 1 FAILED → completed_stages returns exactly 2 IDs."""
        store = m.CheckpointStore()
        store.save_stage("run-1", "stage-a", m.StageStatus.COMPLETED)
        store.save_stage("run-1", "stage-b", m.StageStatus.COMPLETED)
        store.save_stage("run-1", "stage-c", m.StageStatus.FAILED)
        completed = store.completed_stages("run-1")
        assert len(completed) == 2
        assert set(completed) == {"stage-a", "stage-b"}

    def test_separate_runs_independent(self, m):
        """Same stage_id, different run_ids → independent statuses."""
        store = m.CheckpointStore()
        store.save_stage("run-1", "stage-a", m.StageStatus.COMPLETED)
        store.save_stage("run-2", "stage-a", m.StageStatus.FAILED)
        assert store.get_stage_status("run-1", "stage-a") == m.StageStatus.COMPLETED
        assert store.get_stage_status("run-2", "stage-a") == m.StageStatus.FAILED


# ===========================================================================
# TestDataQualityValidator
# ===========================================================================


class TestDataQualityValidator:
    """Tests for DataQualityValidator covering all five rule types."""

    # --- COMPLETENESS ---

    def test_completeness_rule_passes(self, m):
        """Record has all required_fields → QualityReport.all_passed is True."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            description="need name and age",
            required_fields=["name", "age"],
        )
        validator.add_rule(rule)
        records = [_record(m, "rec-1", {"name": "Alice", "age": 30})]
        reports = validator.validate(records)
        assert len(reports) == 1
        assert reports[0].all_passed is True

    def test_completeness_rule_fails_missing_field(self, m):
        """Record missing required field → failed > 0."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            description="need name and age",
            required_fields=["name", "age"],
        )
        validator.add_rule(rule)
        records = [_record(m, "rec-1", {"name": "Alice"})]  # missing "age"
        reports = validator.validate(records)
        assert reports[0].failed > 0
        assert "rec-1" in reports[0].failed_record_ids

    # --- UNIQUENESS ---

    def test_uniqueness_rule_passes(self, m):
        """All records have unique key_field → all_passed."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r2",
            rule_type=m.QualityRuleType.UNIQUENESS,
            description="unique id",
            key_field="id",
        )
        validator.add_rule(rule)
        records = [
            _record(m, "rec-1", {"id": "A"}),
            _record(m, "rec-2", {"id": "B"}),
            _record(m, "rec-3", {"id": "C"}),
        ]
        reports = validator.validate(records)
        assert reports[0].all_passed is True

    def test_uniqueness_rule_fails_duplicate(self, m):
        """Duplicate key_field values → failed_record_ids not empty."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r2",
            rule_type=m.QualityRuleType.UNIQUENESS,
            description="unique id",
            key_field="id",
        )
        validator.add_rule(rule)
        records = [
            _record(m, "rec-1", {"id": "A"}),
            _record(m, "rec-2", {"id": "A"}),  # duplicate
            _record(m, "rec-3", {"id": "C"}),
        ]
        reports = validator.validate(records)
        assert len(reports[0].failed_record_ids) > 0

    # --- RANGE ---

    def test_range_rule_passes(self, m):
        """Values within min/max → all_passed."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r3",
            rule_type=m.QualityRuleType.RANGE,
            description="score 0-100",
            field_name="score",
            min_value=0.0,
            max_value=100.0,
        )
        validator.add_rule(rule)
        records = [
            _record(m, "rec-1", {"score": 50}),
            _record(m, "rec-2", {"score": 0}),
            _record(m, "rec-3", {"score": 100}),
        ]
        reports = validator.validate(records)
        assert reports[0].all_passed is True

    def test_range_rule_fails_out_of_bounds(self, m):
        """Value outside range → failed > 0."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r3",
            rule_type=m.QualityRuleType.RANGE,
            description="score 0-100",
            field_name="score",
            min_value=0.0,
            max_value=100.0,
        )
        validator.add_rule(rule)
        records = [
            _record(m, "rec-1", {"score": 150}),  # out of range
        ]
        reports = validator.validate(records)
        assert reports[0].failed > 0

    # --- REGEX ---

    def test_regex_rule_passes(self, m):
        """Values match pattern → all_passed."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r4",
            rule_type=m.QualityRuleType.REGEX,
            description="email format",
            field_name="email",
            pattern=r"[^@]+@[^@]+\.[^@]+",
        )
        validator.add_rule(rule)
        records = [
            _record(m, "rec-1", {"email": "alice@example.com"}),
            _record(m, "rec-2", {"email": "bob@test.org"}),
        ]
        reports = validator.validate(records)
        assert reports[0].all_passed is True

    # --- QUALITY GATE ---

    def test_quality_gate_raises_on_threshold(self, m):
        """validate_with_threshold with failing records and threshold=1.0 → raises QualityGateError."""
        validator = m.DataQualityValidator()
        rule = m.QualityRule(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            description="need name",
            required_fields=["name"],
        )
        validator.add_rule(rule)
        records = [
            _record(m, "rec-1", {"name": "Alice"}),
            _record(m, "rec-2", {}),  # missing "name"
        ]
        with pytest.raises(m.QualityGateError):
            validator.validate_with_threshold(records, min_pass_rate=1.0)


# ===========================================================================
# TestQualityReport
# ===========================================================================


class TestQualityReport:
    """Tests for QualityReport computed properties."""

    def test_pass_rate_calculation(self, m):
        """passed=8, failed=2, total=10 → pass_rate == 0.8."""
        report = m.QualityReport(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            passed=8,
            failed=2,
            total=10,
        )
        assert report.pass_rate == pytest.approx(0.8)

    def test_all_passed_property(self, m):
        """failed=0 → all_passed is True."""
        report = m.QualityReport(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            passed=5,
            failed=0,
            total=5,
        )
        assert report.all_passed is True

    def test_not_all_passed(self, m):
        """failed=1 → all_passed is False."""
        report = m.QualityReport(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            passed=4,
            failed=1,
            total=5,
        )
        assert report.all_passed is False

    def test_pass_rate_empty_total(self, m):
        """total=0 → pass_rate == 1.0 (vacuously successful)."""
        report = m.QualityReport(
            rule_id="r1",
            rule_type=m.QualityRuleType.COMPLETENESS,
            passed=0,
            failed=0,
            total=0,
        )
        assert report.pass_rate == pytest.approx(1.0)


# ===========================================================================
# TestETLPipeline
# ===========================================================================


class TestETLPipeline:
    """Tests for ETLPipeline: stage execution, checkpointing, metrics, and quality gates."""

    def _make_records(self, m, n, prefix="rec"):
        return [
            m.DataRecord(record_id=f"{prefix}-{i}", payload=f"value-{i}")
            for i in range(n)
        ]

    def test_single_stage_completes(self, m):
        """Extract 3 records, no transform, no load → stage status COMPLETED."""
        store = m.CheckpointStore()
        records = self._make_records(m, 3)
        pipeline = m.ETLPipeline("pipe-1", checkpoint_store=store)
        stage = m.PipelineStage(
            stage_id="s1",
            extract_fn=lambda: records,
        )
        pipeline.add_stage(stage)
        run_id = "run-001"
        pipeline.run(run_id=run_id)
        assert store.get_stage_status(run_id, "s1") == m.StageStatus.COMPLETED

    def test_all_stage_ids_in_history(self, m):
        """3-stage pipeline → metrics.stage_durations has 3 keys."""
        pipeline = m.ETLPipeline("pipe-2")
        for i in range(3):
            pipeline.add_stage(
                m.PipelineStage(
                    stage_id=f"s{i}",
                    extract_fn=lambda i=i: self._make_records(m, 2, prefix=f"s{i}"),
                )
            )
        metrics = pipeline.run(run_id="run-002")
        assert len(metrics.stage_durations) == 3
        assert set(metrics.stage_durations.keys()) == {"s0", "s1", "s2"}

    def test_records_extracted_count(self, m):
        """Extract 5 records → metrics.records_extracted == 5."""
        records = self._make_records(m, 5)
        pipeline = m.ETLPipeline("pipe-3")
        pipeline.add_stage(
            m.PipelineStage(stage_id="s1", extract_fn=lambda: records)
        )
        metrics = pipeline.run(run_id="run-003")
        assert metrics.records_extracted == 5

    def test_transform_applied(self, m):
        """Transform stage doubles payload value → loaded records have doubled payloads."""
        loaded_records = []

        def extract():
            return [
                m.DataRecord(record_id=f"rec-{i}", payload={"val": i})
                for i in range(3)
            ]

        def transform(recs):
            return [
                m.DataRecord(record_id=r.record_id, payload={"val": r.payload["val"] * 2})
                for r in recs
            ]

        def load(recs):
            loaded_records.extend(recs)
            return len(recs)

        pipeline = m.ETLPipeline("pipe-4")
        pipeline.add_stage(
            m.PipelineStage(
                stage_id="s1",
                extract_fn=extract,
                transform_fn=transform,
                load_fn=load,
            )
        )
        pipeline.run(run_id="run-004")
        assert len(loaded_records) == 3
        assert loaded_records[0].payload["val"] == 0
        assert loaded_records[1].payload["val"] == 2
        assert loaded_records[2].payload["val"] == 4

    def test_checkpoint_skip_on_resume(self, m):
        """Run pipeline twice with same run_id → second run has stage in stage_durations with 0.0 duration (skipped)."""
        store = m.CheckpointStore()
        extract_calls = {"count": 0}

        def counting_extract():
            extract_calls["count"] += 1
            return self._make_records(m, 2, prefix="ckpt")

        pipeline = m.ETLPipeline("pipe-5", checkpoint_store=store)
        pipeline.add_stage(
            m.PipelineStage(
                stage_id="s1",
                extract_fn=counting_extract,
                skip_on_checkpoint=True,
            )
        )
        run_id = "run-005"
        pipeline.run(run_id=run_id)
        first_count = extract_calls["count"]

        # Second run with same run_id — stage already COMPLETED
        pipeline.run(run_id=run_id)
        second_count = extract_calls["count"]

        assert first_count == 1
        # Extract should NOT have been called again on the second run
        assert second_count == 1

    def test_quality_gate_failure_raises(self, m):
        """Stage with failing COMPLETENESS rule + threshold=1.0 → raises QualityGateError."""
        def extract():
            return [
                m.DataRecord(record_id="rec-1", payload={"name": "Alice"}),
                m.DataRecord(record_id="rec-2", payload={}),  # missing "name"
            ]

        rule = m.QualityRule(
            rule_id="completeness-check",
            rule_type=m.QualityRuleType.COMPLETENESS,
            description="name required",
            required_fields=["name"],
        )
        stage = m.PipelineStage(
            stage_id="s1",
            extract_fn=extract,
            quality_rules=[rule],
            min_quality_pass_rate=1.0,
        )
        pipeline = m.ETLPipeline("pipe-6")
        pipeline.add_stage(stage)
        with pytest.raises(m.QualityGateError):
            pipeline.run(run_id="run-006")

    def test_stage_ids_returned(self, m):
        """pipeline.stage_ids() returns list of stage IDs in order."""
        pipeline = m.ETLPipeline("pipe-7")
        for sid in ["alpha", "beta", "gamma"]:
            pipeline.add_stage(m.PipelineStage(stage_id=sid))
        assert pipeline.stage_ids() == ["alpha", "beta", "gamma"]

    def test_fluent_add_stage_returns_pipeline(self, m):
        """add_stage() returns the pipeline itself for method chaining."""
        pipeline = m.ETLPipeline("pipe-8")
        result = pipeline.add_stage(m.PipelineStage(stage_id="s1"))
        assert result is pipeline
