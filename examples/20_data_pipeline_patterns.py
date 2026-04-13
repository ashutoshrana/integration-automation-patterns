"""
20_data_pipeline_patterns.py — ETL (Extract-Transform-Load) data pipeline
patterns with checkpointing for fault-tolerant resumable pipelines, data
quality validation rules, and pipeline observability metrics.

Demonstrates four complementary patterns that together implement a production-
grade ETL pipeline engine suitable for batch data integration workflows:

    Pattern 1 — Checkpointing for Fault-Tolerant Resumability:
                Persists pipeline progress (stage completion status and
                processed record IDs) in a CheckpointStore so that a
                restarted pipeline can skip work already done instead of
                re-processing from scratch. This mirrors the checkpoint
                semantics of Apache Spark Structured Streaming checkpoints,
                AWS Glue job bookmarks, and Apache Flink state backends —
                each of which maintains exactly-once or at-least-once
                delivery guarantees by tracking the frontier of processed
                data across restarts.

    Pattern 2 — Data Quality Validation with Quality Gates:
                Enforces five rule types (completeness, uniqueness, range,
                regex, custom predicate) against every batch of records
                before the load step executes. Each rule emits a
                QualityReport with pass/fail counts and the IDs of failing
                records. A quality gate raises QualityGateError when any
                rule's pass rate falls below a configurable threshold.
                This pattern implements the validation layer described in
                the Great Expectations and dbt test frameworks, and mirrors
                the data quality rules available in AWS Glue DataBrew and
                Azure Data Factory's validation activities.

    Pattern 3 — Parallel Stage Execution (fanout within a stage):
                Although ETLPipeline executes stages sequentially (preserving
                dependency order), each stage's extract / transform / load
                callables are free to implement internal parallelism.  The
                pipeline correctly measures per-stage wall-clock duration
                regardless of the callable's internal implementation, making
                it straightforward to swap in ThreadPoolExecutor-based or
                asyncio-based callables without changing the pipeline engine.

    Pattern 4 — Pipeline Observability Metrics:
                PipelineMetrics captures record-level counters (extracted,
                transformed, loaded, failed), per-stage wall-clock durations,
                all QualityReport objects, and overall run timing. These
                metrics are the raw material for dashboards comparable to
                those produced by Apache Airflow's task-instance metrics,
                Prefect's flow-run observability, and dbt's run results JSON
                artefact.

Relationship to commercial pipeline frameworks:
    - Apache Spark       : ETLPipeline ≈ SparkSession with DataFrame
                           transformations; CheckpointStore ≈ Spark checkpoint
                           directory; QualityReport ≈ deequ ConstraintResult.
    - AWS Glue           : PipelineStage ≈ Glue Job script; CheckpointStore ≈
                           Glue Job Bookmark; QualityRule ≈ Glue DataBrew rule.
    - Apache Airflow     : ETLPipeline ≈ DAG; PipelineStage ≈ Operator;
                           PipelineMetrics ≈ TaskInstance XCom + Airflow
                           metrics.
    - dbt                : QualityRule ≈ dbt test (not_null, unique, accepted
                           values, custom); QualityGateError ≈ test failure
                           blocking downstream models.
    - Great Expectations : DataQualityValidator ≈ Expectation Suite;
                           QualityReport ≈ ValidationResult.

No external dependencies required.

Run:
    python examples/20_data_pipeline_patterns.py
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class StageStatus(Enum):
    """All possible lifecycle states of a pipeline stage within a run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"  # Already processed (checkpoint exists)


class QualityRuleType(Enum):
    """Discriminator for the five built-in data quality rule varieties."""

    COMPLETENESS = "completeness"  # required fields present in every record
    UNIQUENESS = "uniqueness"       # no duplicate values for a key field
    RANGE = "range"                 # numeric field value within [min, max]
    REGEX = "regex"                 # field value matches a regex pattern
    CUSTOM = "custom"               # caller-supplied predicate: record -> bool


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DataPipelineError(Exception):
    """Raised when a pipeline stage fails fatally and cannot recover."""


class QualityGateError(Exception):
    """Raised when a data quality rule's pass rate falls below the threshold."""


# ---------------------------------------------------------------------------
# DataRecord dataclass
# ---------------------------------------------------------------------------


@dataclass
class DataRecord:
    """
    Immutable unit of data flowing through the pipeline.

    Each record carries a stable *record_id* (used for deduplication via the
    CheckpointStore), an arbitrary *payload* (dict, string, or any Python
    object), and an open-ended *metadata* dict populated by stages as the
    record moves through the pipeline.
    """

    record_id: str
    payload: Any
    metadata: dict = field(default_factory=dict)

    def with_metadata(self, **kwargs: Any) -> "DataRecord":
        """Return a new DataRecord with the given key/value pairs merged into metadata.

        The original record is not mutated — a fresh DataRecord is returned
        with the same record_id and payload but an updated metadata dict.

        Args:
            **kwargs: Arbitrary key/value pairs to merge into metadata.

        Returns:
            New DataRecord with merged metadata.
        """
        new_meta = {**self.metadata, **kwargs}
        return DataRecord(record_id=self.record_id, payload=self.payload, metadata=new_meta)


# ---------------------------------------------------------------------------
# QualityRule dataclass
# ---------------------------------------------------------------------------


@dataclass
class QualityRule:
    """
    Declarative specification of a single data quality constraint.

    The *rule_type* field selects which validation algorithm is applied.
    Only the fields relevant to the chosen rule_type need to be populated;
    the others are ignored.

    Field usage by rule_type:
        COMPLETENESS : *required_fields* — list of field names that must be
                       present as keys in each record's payload dict.
        UNIQUENESS   : *key_field* — the payload dict key whose values must
                       be unique across the entire batch.
        RANGE        : *field_name*, *min_value*, *max_value* — the payload
                       dict key whose numeric value must satisfy
                       min_value <= value <= max_value.
        REGEX        : *field_name*, *pattern* — the payload dict key whose
                       string value must fully match the regex pattern.
        CUSTOM       : *predicate* — a callable(DataRecord) -> bool that
                       returns True when the record passes.
    """

    rule_id: str
    rule_type: QualityRuleType
    description: str
    # COMPLETENESS
    required_fields: list = field(default_factory=list)
    # UNIQUENESS
    key_field: str = ""
    # RANGE
    field_name: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    # REGEX
    pattern: str = ""
    # CUSTOM
    predicate: Optional[Any] = None  # callable: DataRecord -> bool


# ---------------------------------------------------------------------------
# QualityReport dataclass
# ---------------------------------------------------------------------------


@dataclass
class QualityReport:
    """
    Result of applying a single QualityRule to a batch of DataRecords.

    Captures how many records passed and failed, plus the IDs of every
    failing record so that downstream processes can quarantine or re-route
    bad data.
    """

    rule_id: str
    rule_type: QualityRuleType
    passed: int
    failed: int
    total: int
    failed_record_ids: list = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        """Fraction of records that passed the rule; 1.0 if the batch is empty."""
        return self.passed / self.total if self.total > 0 else 1.0

    @property
    def all_passed(self) -> bool:
        """True when every record in the batch satisfied the rule."""
        return self.failed == 0


# ---------------------------------------------------------------------------
# CheckpointStore
# ---------------------------------------------------------------------------


class CheckpointStore:
    """
    In-memory checkpoint store enabling fault-tolerant pipeline resumability.

    Persists two kinds of progress information for each pipeline run:

    1. **Stage completion status** — whether each stage is PENDING, RUNNING,
       COMPLETED, or FAILED.  A stage whose saved status is COMPLETED can be
       skipped entirely on a restart.

    2. **Processed record IDs** — the set of record_ids whose load step has
       already been committed for this run.  On restart, the pipeline calls
       *get_unprocessed()* to filter out records that were already delivered,
       achieving at-least-once semantics with an idempotent load function.

    In production this store would be backed by a durable medium such as
    Redis, Amazon DynamoDB, or a relational database table. The in-memory
    implementation here is sufficient for single-process use and unit testing.
    """

    def __init__(self) -> None:
        self._checkpoints: dict = {}   # run_id -> {stage_id: StageStatus}
        self._processed_ids: dict = {} # run_id -> set[str]

    def save_stage(self, run_id: str, stage_id: str, status: StageStatus) -> None:
        """Persist the completion status of *stage_id* for *run_id*.

        Args:
            run_id:   Unique identifier for the pipeline run.
            stage_id: Identifier of the stage whose status is being saved.
            status:   The StageStatus to persist.
        """
        if run_id not in self._checkpoints:
            self._checkpoints[run_id] = {}
        self._checkpoints[run_id][stage_id] = status

    def get_stage_status(self, run_id: str, stage_id: str) -> Optional[StageStatus]:
        """Return the saved StageStatus for *stage_id* in *run_id*, or None.

        Returns None when no checkpoint has been written for this
        (run_id, stage_id) pair — meaning the stage has not yet been
        started in this run.

        Args:
            run_id:   Unique identifier for the pipeline run.
            stage_id: Identifier of the stage to look up.

        Returns:
            Saved StageStatus or None if no checkpoint exists.
        """
        return self._checkpoints.get(run_id, {}).get(stage_id)

    def mark_processed(self, run_id: str, record_ids: list) -> None:
        """Mark a collection of record IDs as successfully loaded in *run_id*.

        Subsequent calls to *is_processed()* or *get_unprocessed()* will
        treat these IDs as already done.

        Args:
            run_id:     Unique identifier for the pipeline run.
            record_ids: Iterable of record_id strings to mark as processed.
        """
        if run_id not in self._processed_ids:
            self._processed_ids[run_id] = set()
        self._processed_ids[run_id].update(record_ids)

    def is_processed(self, run_id: str, record_id: str) -> bool:
        """Return True if *record_id* was already processed in *run_id*.

        Args:
            run_id:    Unique identifier for the pipeline run.
            record_id: The record_id to check.

        Returns:
            True if the record was previously marked as processed.
        """
        return record_id in self._processed_ids.get(run_id, set())

    def get_unprocessed(self, run_id: str, records: list) -> list:
        """Filter *records* to only those not yet processed in *run_id*.

        Used by the pipeline engine to skip records that were already
        delivered to the load target in a previous run attempt.

        Args:
            run_id:  Unique identifier for the pipeline run.
            records: Full list of DataRecord objects to filter.

        Returns:
            Subset of *records* whose record_id has not been processed.
        """
        processed = self._processed_ids.get(run_id, set())
        return [r for r in records if r.record_id not in processed]

    def clear_run(self, run_id: str) -> None:
        """Remove all checkpoint data for *run_id*, forcing full re-execution.

        Args:
            run_id: Unique identifier for the pipeline run to clear.
        """
        self._checkpoints.pop(run_id, None)
        self._processed_ids.pop(run_id, None)

    def completed_stages(self, run_id: str) -> list:
        """Return the stage IDs whose status is COMPLETED for *run_id*.

        Args:
            run_id: Unique identifier for the pipeline run.

        Returns:
            List of stage_id strings with COMPLETED status.
        """
        stages = self._checkpoints.get(run_id, {})
        return [sid for sid, status in stages.items() if status == StageStatus.COMPLETED]


# ---------------------------------------------------------------------------
# DataQualityValidator
# ---------------------------------------------------------------------------


class DataQualityValidator:
    """
    Validates a batch of DataRecords against a set of QualityRules.

    Rules are registered via the fluent *add_rule()* API and evaluated in
    registration order by *validate()*.  Each call to *validate()* produces
    one QualityReport per registered rule.

    *validate_with_threshold()* extends *validate()* by raising
    QualityGateError when any rule's pass_rate falls below *min_pass_rate*,
    blocking the load step from executing with dirty data.
    """

    def __init__(self) -> None:
        self._rules: list = []  # List[QualityRule]

    def add_rule(self, rule: QualityRule) -> "DataQualityValidator":
        """Register a quality rule. Returns self to support method chaining.

        Args:
            rule: The QualityRule to add.

        Returns:
            This DataQualityValidator instance.
        """
        self._rules.append(rule)
        return self

    def validate(self, records: list) -> list:
        """Validate *records* against all registered rules.

        Each rule type is evaluated independently:

            COMPLETENESS — a record passes when every name in
                           rule.required_fields is present as a key in its
                           payload dict. Non-dict payloads always fail.

            UNIQUENESS   — values of rule.key_field are collected across the
                           batch; records whose value appears more than once
                           are marked as failures. Non-dict payloads always
                           fail.

            RANGE        — rule.field_name is looked up in the payload dict;
                           the record passes when the value is a number
                           satisfying min_value <= value <= max_value.
                           Missing field or non-numeric value → failure.

            REGEX        — rule.field_name is looked up in the payload dict;
                           the record passes when re.match(pattern, str(value))
                           returns a match. Missing field → failure.

            CUSTOM       — rule.predicate(record) is called; True → pass,
                           False or raised exception → failure.

        Args:
            records: List of DataRecord objects to validate.

        Returns:
            List of QualityReport objects, one per registered rule.
        """
        reports = []
        for rule in self._rules:
            report = self._apply_rule(rule, records)
            reports.append(report)
        return reports

    def validate_with_threshold(
        self, records: list, min_pass_rate: float = 1.0
    ) -> list:
        """Validate records and raise QualityGateError if any rule falls below threshold.

        Args:
            records:       List of DataRecord objects to validate.
            min_pass_rate: Minimum acceptable pass rate in [0.0, 1.0].
                           Default 1.0 means all records must pass every rule.

        Returns:
            List of QualityReport objects (same as validate()).

        Raises:
            QualityGateError: If any rule's pass_rate < min_pass_rate.
        """
        reports = self.validate(records)
        failing = [r for r in reports if r.pass_rate < min_pass_rate]
        if failing:
            details = "; ".join(
                f"rule '{r.rule_id}' pass_rate={r.pass_rate:.2%} "
                f"(failed {r.failed}/{r.total} records)"
                for r in failing
            )
            raise QualityGateError(
                f"Data quality gate failed — {len(failing)} rule(s) below "
                f"threshold {min_pass_rate:.2%}: {details}"
            )
        return reports

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_rule(self, rule: QualityRule, records: list) -> QualityReport:
        """Dispatch to the appropriate validation algorithm and build a report."""
        if rule.rule_type == QualityRuleType.COMPLETENESS:
            return self._check_completeness(rule, records)
        if rule.rule_type == QualityRuleType.UNIQUENESS:
            return self._check_uniqueness(rule, records)
        if rule.rule_type == QualityRuleType.RANGE:
            return self._check_range(rule, records)
        if rule.rule_type == QualityRuleType.REGEX:
            return self._check_regex(rule, records)
        if rule.rule_type == QualityRuleType.CUSTOM:
            return self._check_custom(rule, records)
        # Unreachable with a well-formed enum, but keeps the linter happy.
        raise DataPipelineError(f"Unknown rule type: {rule.rule_type}")  # pragma: no cover

    def _check_completeness(self, rule: QualityRule, records: list) -> QualityReport:
        """COMPLETENESS — every required field must be present in payload."""
        passed_ids: list = []
        failed_ids: list = []
        for record in records:
            if not isinstance(record.payload, dict):
                failed_ids.append(record.record_id)
                continue
            missing = [f for f in rule.required_fields if f not in record.payload]
            if missing:
                failed_ids.append(record.record_id)
            else:
                passed_ids.append(record.record_id)
        return QualityReport(
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
            passed=len(passed_ids),
            failed=len(failed_ids),
            total=len(records),
            failed_record_ids=failed_ids,
        )

    def _check_uniqueness(self, rule: QualityRule, records: list) -> QualityReport:
        """UNIQUENESS — key_field values must be unique across the batch."""
        # First pass: collect all values and find duplicates.
        seen: dict = {}  # value -> first record_id
        duplicated_values: set = set()
        for record in records:
            if not isinstance(record.payload, dict):
                continue
            value = record.payload.get(rule.key_field)
            if value is None:
                continue
            if value in seen:
                duplicated_values.add(value)
            else:
                seen[value] = record.record_id

        # Second pass: build pass/fail lists.
        failed_ids: list = []
        passed_ids: list = []
        for record in records:
            if not isinstance(record.payload, dict):
                failed_ids.append(record.record_id)
                continue
            value = record.payload.get(rule.key_field)
            if value is None or value in duplicated_values:
                failed_ids.append(record.record_id)
            else:
                passed_ids.append(record.record_id)
        return QualityReport(
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
            passed=len(passed_ids),
            failed=len(failed_ids),
            total=len(records),
            failed_record_ids=failed_ids,
        )

    def _check_range(self, rule: QualityRule, records: list) -> QualityReport:
        """RANGE — field_name value must be a number within [min_value, max_value]."""
        failed_ids: list = []
        passed_ids: list = []
        for record in records:
            try:
                if not isinstance(record.payload, dict):
                    raise ValueError("payload is not a dict")
                raw = record.payload.get(rule.field_name)
                if raw is None:
                    raise ValueError(f"field '{rule.field_name}' missing")
                value = float(raw)
                in_range = True
                if rule.min_value is not None and value < rule.min_value:
                    in_range = False
                if rule.max_value is not None and value > rule.max_value:
                    in_range = False
                if in_range:
                    passed_ids.append(record.record_id)
                else:
                    failed_ids.append(record.record_id)
            except (TypeError, ValueError):
                failed_ids.append(record.record_id)
        return QualityReport(
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
            passed=len(passed_ids),
            failed=len(failed_ids),
            total=len(records),
            failed_record_ids=failed_ids,
        )

    def _check_regex(self, rule: QualityRule, records: list) -> QualityReport:
        """REGEX — str(field_name value) must match the pattern."""
        failed_ids: list = []
        passed_ids: list = []
        compiled = re.compile(rule.pattern)
        for record in records:
            try:
                if not isinstance(record.payload, dict):
                    raise ValueError("payload is not a dict")
                raw = record.payload.get(rule.field_name)
                if raw is None:
                    raise ValueError(f"field '{rule.field_name}' missing")
                if compiled.match(str(raw)):
                    passed_ids.append(record.record_id)
                else:
                    failed_ids.append(record.record_id)
            except (TypeError, ValueError):
                failed_ids.append(record.record_id)
        return QualityReport(
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
            passed=len(passed_ids),
            failed=len(failed_ids),
            total=len(records),
            failed_record_ids=failed_ids,
        )

    def _check_custom(self, rule: QualityRule, records: list) -> QualityReport:
        """CUSTOM — rule.predicate(record) must return True for each record."""
        failed_ids: list = []
        passed_ids: list = []
        for record in records:
            try:
                if rule.predicate is None:
                    raise ValueError("predicate is None")
                result = rule.predicate(record)
                if result:
                    passed_ids.append(record.record_id)
                else:
                    failed_ids.append(record.record_id)
            except Exception:  # noqa: BLE001
                failed_ids.append(record.record_id)
        return QualityReport(
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
            passed=len(passed_ids),
            failed=len(failed_ids),
            total=len(records),
            failed_record_ids=failed_ids,
        )


# ---------------------------------------------------------------------------
# PipelineStage dataclass
# ---------------------------------------------------------------------------


@dataclass
class PipelineStage:
    """
    Describes one ETL stage: extract, transform, load callables plus quality rules.

    The three callables are optional to support partial pipelines (e.g. an
    extract-only stage that writes to an intermediate store, or a load-only
    stage that consumes pre-extracted records).

    Quality rules registered on a stage are evaluated *after* transform and
    *before* load, giving the pipeline an opportunity to reject bad data
    before it reaches the destination system.
    """

    stage_id: str
    extract_fn: Optional[Any] = None    # callable() -> list[DataRecord]
    transform_fn: Optional[Any] = None  # callable(list[DataRecord]) -> list[DataRecord]
    load_fn: Optional[Any] = None       # callable(list[DataRecord]) -> int
    quality_rules: list = field(default_factory=list)  # List[QualityRule]
    min_quality_pass_rate: float = 1.0
    skip_on_checkpoint: bool = True     # Skip if checkpoint says COMPLETED


# ---------------------------------------------------------------------------
# PipelineMetrics dataclass
# ---------------------------------------------------------------------------


@dataclass
class PipelineMetrics:
    """
    Observability metrics for a single pipeline run.

    Captures record-level counters at each ETL phase, all QualityReport
    objects produced by quality gates, per-stage wall-clock durations, and
    overall run timing.  Together these fields are sufficient to populate an
    operational dashboard or emit structured log events for ingestion by a
    monitoring system.
    """

    run_id: str
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    quality_reports: list = field(default_factory=list)   # List[QualityReport]
    stage_durations: dict = field(default_factory=dict)   # stage_id -> float (seconds)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def total_duration(self) -> float:
        """Wall-clock seconds from pipeline start to completion; 0.0 if not finished."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return 0.0

    @property
    def success_rate(self) -> float:
        """Fraction of extracted records that were successfully loaded.

        Returns 1.0 when no records were extracted (vacuously successful).
        """
        total = self.records_extracted
        return (total - self.records_failed) / total if total > 0 else 1.0

    def record_stage_duration(self, stage_id: str, duration: float) -> None:
        """Store the wall-clock duration of a stage in seconds.

        Args:
            stage_id: Identifier of the stage that just completed.
            duration: Elapsed time in seconds for that stage.
        """
        self.stage_durations[stage_id] = duration


# ---------------------------------------------------------------------------
# ETLPipeline
# ---------------------------------------------------------------------------


class ETLPipeline:
    """
    Orchestrates sequential ETL stages with checkpointing and quality gates.

    Stages are executed in registration order.  For each stage the pipeline:

        1. Consults the CheckpointStore — if the stage is already COMPLETED
           and *skip_on_checkpoint* is True, the stage is skipped entirely.
        2. Calls *extract_fn()* to produce the initial list of DataRecords.
        3. Filters out records already processed in this run (resumability).
        4. Calls *transform_fn(records)* to reshape the data.
        5. Runs all *quality_rules* via DataQualityValidator; raises
           QualityGateError if any rule falls below *min_quality_pass_rate*.
        6. Calls *load_fn(records)* to deliver records to the target; the
           function returns the number of records actually loaded.
        7. Marks all record IDs as processed and the stage as COMPLETED in
           the CheckpointStore.

    On DataPipelineError or QualityGateError the stage is marked FAILED in
    the checkpoint store and the exception is re-raised so the caller can
    decide whether to retry, alert, or skip.
    """

    def __init__(
        self,
        pipeline_id: str,
        checkpoint_store: Optional[CheckpointStore] = None,
    ) -> None:
        """Initialise the pipeline with an optional external CheckpointStore.

        Args:
            pipeline_id:       Human-readable identifier for this pipeline.
            checkpoint_store:  Shared CheckpointStore instance. A new
                               in-memory store is created when None.
        """
        self.pipeline_id = pipeline_id
        self._stages: list = []  # List[PipelineStage]
        self._checkpoint_store = checkpoint_store or CheckpointStore()

    def add_stage(self, stage: PipelineStage) -> "ETLPipeline":
        """Append a stage to the pipeline. Returns self for fluent API.

        Args:
            stage: The PipelineStage to add.

        Returns:
            This ETLPipeline instance.
        """
        self._stages.append(stage)
        return self

    def run(self, run_id: Optional[str] = None) -> PipelineMetrics:
        """Execute all registered stages sequentially.

        Algorithm (per stage):
            a. Check checkpoint — COMPLETED + skip_on_checkpoint → SKIPPED.
            b. Mark stage RUNNING in checkpoint.
            c. Extract: call extract_fn() → list of DataRecords.
            d. Filter records already processed via checkpoint (resumability).
            e. Transform: call transform_fn(records) if provided.
            f. Validate quality with stage rules and min_quality_pass_rate.
            g. Load: call load_fn(records) → int of records loaded.
            h. Mark processed record IDs in checkpoint.
            i. Mark stage COMPLETED; record stage duration in metrics.

        On DataPipelineError or QualityGateError:
            - Stage is marked FAILED in the checkpoint store.
            - Exception is re-raised immediately (no subsequent stages run).

        Args:
            run_id: Caller-supplied run identifier for checkpoint keying.
                    A UUID is generated when None.

        Returns:
            PipelineMetrics populated with counts, durations, and quality
            reports for this run.

        Raises:
            DataPipelineError: When a stage's extract, transform, or load
                               callable raises fatally.
            QualityGateError:  When data quality validation fails to meet
                               the stage's min_quality_pass_rate threshold.
        """
        if run_id is None:
            run_id = str(uuid.uuid4())

        metrics = PipelineMetrics(run_id=run_id)
        metrics.started_at = time.time()

        for stage in self._stages:
            stage_start = time.time()

            # ----------------------------------------------------------
            # Checkpoint — skip if already completed
            # ----------------------------------------------------------
            saved_status = self._checkpoint_store.get_stage_status(run_id, stage.stage_id)
            if saved_status == StageStatus.COMPLETED and stage.skip_on_checkpoint:
                metrics.record_stage_duration(stage.stage_id, 0.0)
                continue

            # ----------------------------------------------------------
            # Mark stage as RUNNING
            # ----------------------------------------------------------
            self._checkpoint_store.save_stage(run_id, stage.stage_id, StageStatus.RUNNING)

            try:
                # ------------------------------------------------------
                # Extract
                # ------------------------------------------------------
                if stage.extract_fn is not None:
                    records: list = stage.extract_fn()
                    if not isinstance(records, list):
                        records = list(records)
                else:
                    records = []

                metrics.records_extracted += len(records)

                # ------------------------------------------------------
                # Filter already-processed records (resumability)
                # ------------------------------------------------------
                records = self._checkpoint_store.get_unprocessed(run_id, records)

                # ------------------------------------------------------
                # Transform
                # ------------------------------------------------------
                if stage.transform_fn is not None:
                    records = stage.transform_fn(records)
                    if not isinstance(records, list):
                        records = list(records)

                metrics.records_transformed += len(records)

                # ------------------------------------------------------
                # Quality validation
                # ------------------------------------------------------
                if stage.quality_rules:
                    validator = DataQualityValidator()
                    for rule in stage.quality_rules:
                        validator.add_rule(rule)
                    reports = validator.validate_with_threshold(
                        records, min_pass_rate=stage.min_quality_pass_rate
                    )
                    metrics.quality_reports.extend(reports)

                # ------------------------------------------------------
                # Load
                # ------------------------------------------------------
                loaded_count = 0
                if stage.load_fn is not None and records:
                    loaded_count = stage.load_fn(records)
                    if not isinstance(loaded_count, int):
                        loaded_count = int(loaded_count)
                elif records:
                    # No load_fn — treat all transformed records as loaded.
                    loaded_count = len(records)

                metrics.records_loaded += loaded_count
                # Any records that were not loaded are counted as failed.
                unloaded = len(records) - loaded_count
                metrics.records_failed += max(0, unloaded)

                # ------------------------------------------------------
                # Mark record IDs as processed
                # ------------------------------------------------------
                self._checkpoint_store.mark_processed(
                    run_id, [r.record_id for r in records]
                )

                # ------------------------------------------------------
                # Mark stage COMPLETED
                # ------------------------------------------------------
                self._checkpoint_store.save_stage(
                    run_id, stage.stage_id, StageStatus.COMPLETED
                )

            except (DataPipelineError, QualityGateError):
                # Fatal — persist FAILED status and re-raise.
                self._checkpoint_store.save_stage(
                    run_id, stage.stage_id, StageStatus.FAILED
                )
                stage_duration = time.time() - stage_start
                metrics.record_stage_duration(stage.stage_id, stage_duration)
                metrics.completed_at = time.time()
                raise
            except Exception as exc:
                # Unexpected error — wrap in DataPipelineError.
                self._checkpoint_store.save_stage(
                    run_id, stage.stage_id, StageStatus.FAILED
                )
                stage_duration = time.time() - stage_start
                metrics.record_stage_duration(stage.stage_id, stage_duration)
                metrics.completed_at = time.time()
                raise DataPipelineError(
                    f"Stage '{stage.stage_id}' failed with unexpected error: {exc}"
                ) from exc

            stage_duration = time.time() - stage_start
            metrics.record_stage_duration(stage.stage_id, stage_duration)

        metrics.completed_at = time.time()
        return metrics

    def stage_ids(self) -> list:
        """Return the list of registered stage IDs in registration order.

        Returns:
            List of stage_id strings.
        """
        return [s.stage_id for s in self._stages]

    def reset_run(self, run_id: str) -> None:
        """Clear all checkpoint data for *run_id*, forcing full re-execution.

        After calling this method, the next call to *run(run_id)* will
        execute every stage from scratch as if it had never run.

        Args:
            run_id: Unique identifier for the run to reset.
        """
        self._checkpoint_store.clear_run(run_id)


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _make_records(n: int, payload_prefix: str = "value") -> list:
    """Build *n* DataRecords with string payloads for smoke tests.

    record_ids are namespaced under *payload_prefix* to prevent collisions
    between different stages that call this function in the same pipeline run.
    """
    return [
        DataRecord(record_id=f"{payload_prefix}-rec-{i:03d}", payload=f"{payload_prefix}-{i}")
        for i in range(n)
    ]


def _make_dict_records(data: list) -> list:
    """Wrap a list of dicts as DataRecords, using list index as record_id."""
    return [
        DataRecord(record_id=f"rec-{i:03d}", payload=item)
        for i, item in enumerate(data)
    ]


def _uppercase_transform(records: list) -> list:
    """Transform stage: upper-case the string payload of every record."""
    return [
        DataRecord(
            record_id=r.record_id,
            payload=r.payload.upper() if isinstance(r.payload, str) else r.payload,
            metadata=r.metadata,
        )
        for r in records
    ]


def _collect_load(destination: list):
    """Return a load_fn that appends records to *destination*."""
    def _load(records: list) -> int:
        destination.extend(records)
        return len(records)
    return _load


# ===========================================================================
# Smoke tests
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_happy_path() -> PipelineMetrics:
    """Scenario 1: 3-stage pipeline — each stage is a self-contained ETL unit."""
    _separator("Scenario 1 — Happy Path (3-stage ETL, all records loaded)")

    # Three independent stages, each with their own extract+transform+load.
    dest_a: list = []
    dest_b: list = []
    dest_c: list = []

    checkpoint_store = CheckpointStore()
    run_id = "run-happy-001"

    pipeline = ETLPipeline("happy-pipeline", checkpoint_store=checkpoint_store)

    # Stage A: 2 records, transform uppercase, load to dest_a
    pipeline.add_stage(PipelineStage(
        stage_id="stage-a",
        extract_fn=lambda: _make_records(2, "alpha"),
        transform_fn=_uppercase_transform,
        load_fn=_collect_load(dest_a),
    ))
    # Stage B: 2 records, no transform, load to dest_b
    pipeline.add_stage(PipelineStage(
        stage_id="stage-b",
        extract_fn=lambda: _make_records(2, "beta"),
        load_fn=_collect_load(dest_b),
    ))
    # Stage C: 1 record, transform uppercase, load to dest_c
    pipeline.add_stage(PipelineStage(
        stage_id="stage-c",
        extract_fn=lambda: _make_records(1, "gamma"),
        transform_fn=_uppercase_transform,
        load_fn=_collect_load(dest_c),
    ))

    metrics = pipeline.run(run_id=run_id)

    print(f"Run ID              : {metrics.run_id}")
    print(f"Records extracted   : {metrics.records_extracted}")
    print(f"Records transformed : {metrics.records_transformed}")
    print(f"Records loaded      : {metrics.records_loaded}")
    print(f"Records failed      : {metrics.records_failed}")
    print(f"Success rate        : {metrics.success_rate:.0%}")
    print(f"Total duration (s)  : {metrics.total_duration:.4f}")
    print(f"Stage durations     : {metrics.stage_durations}")
    print(f"Completed stages    : {checkpoint_store.completed_stages(run_id)}")
    print(f"Stage-A payloads    : {[r.payload for r in dest_a]}")
    print(f"Stage-B payloads    : {[r.payload for r in dest_b]}")
    print(f"Stage-C payloads    : {[r.payload for r in dest_c]}")

    assert metrics.records_extracted == 5, f"Expected 5 extracted, got {metrics.records_extracted}"
    assert metrics.records_loaded == 5, f"Expected 5 loaded, got {metrics.records_loaded}"
    assert metrics.records_failed == 0
    assert all(r.payload == r.payload.upper() for r in dest_a)
    assert all(r.payload == r.payload.upper() for r in dest_c)
    assert set(checkpoint_store.completed_stages(run_id)) == {"stage-a", "stage-b", "stage-c"}
    print("PASS")
    return metrics


def _run_checkpoint_resume() -> None:
    """Scenario 2: Re-run with same run_id — all stages SKIPPED."""
    _separator("Scenario 2 — Checkpoint Resume (all stages already completed)")

    checkpoint_store = CheckpointStore()
    run_id = "run-resume-001"

    # Each stage is a complete ETL unit (extract + load together).
    dest_first: list = []
    pipeline = ETLPipeline("resume-pipeline", checkpoint_store=checkpoint_store)
    pipeline.add_stage(PipelineStage(
        stage_id="ingest-stage",
        extract_fn=lambda: _make_records(3, "data"),
        load_fn=_collect_load(dest_first),
        skip_on_checkpoint=True,
    ))

    # First run — should execute normally and load 3 records.
    pipeline.run(run_id=run_id)
    first_load_count = len(dest_first)

    # Second run with the SAME run_id — stage is already COMPLETED → SKIPPED.
    dest_second: list = []
    pipeline2 = ETLPipeline("resume-pipeline", checkpoint_store=checkpoint_store)
    pipeline2.add_stage(PipelineStage(
        stage_id="ingest-stage",
        extract_fn=lambda: _make_records(3, "data"),
        load_fn=_collect_load(dest_second),
        skip_on_checkpoint=True,
    ))
    metrics2 = pipeline2.run(run_id=run_id)

    print(f"First run loaded    : {first_load_count} record(s)")
    print(f"Second run loaded   : {len(dest_second)} record(s) (should be 0 — skipped)")
    print(f"Completed stages    : {checkpoint_store.completed_stages(run_id)}")
    print(f"Stage durations     : {metrics2.stage_durations}")

    assert first_load_count == 3, f"Expected 3 on first run, got {first_load_count}"
    assert len(dest_second) == 0, (
        f"Expected 0 on second run (stage skipped), got {len(dest_second)}"
    )
    # Skipped stage should record 0.0 duration.
    assert all(d == 0.0 for d in metrics2.stage_durations.values()), (
        "Skipped stages should have 0.0 duration"
    )
    print("PASS")


def _run_quality_gate_failure() -> None:
    """Scenario 3: COMPLETENESS rule fails — QualityGateError raised."""
    _separator("Scenario 3 — Quality Gate Failure (missing required field)")

    records = _make_dict_records([
        {"name": "Alice", "score": 90},
        {"score": 85},           # missing 'name' — should fail
        {"name": "Charlie", "score": 78},
    ])

    completeness_rule = QualityRule(
        rule_id="name-completeness",
        rule_type=QualityRuleType.COMPLETENESS,
        description="Every record must have a 'name' field",
        required_fields=["name"],
    )

    pipeline = ETLPipeline("quality-gate-pipeline")
    pipeline.add_stage(PipelineStage(
        stage_id="validated-load",
        extract_fn=lambda: records,
        quality_rules=[completeness_rule],
        min_quality_pass_rate=1.0,  # strict: every record must pass
    ))

    error_raised = False
    try:
        pipeline.run(run_id="run-quality-001")
    except QualityGateError as exc:
        error_raised = True
        print(f"QualityGateError raised (expected): {exc}")

    assert error_raised, "Expected QualityGateError was not raised"
    print("PASS")


def _run_quality_report() -> None:
    """Scenario 4: RANGE rule applied — report shows pass/fail counts."""
    _separator("Scenario 4 — Quality Report (RANGE rule, some out-of-range)")

    records = _make_dict_records([
        {"value": 50},
        {"value": 101},   # out of range
        {"value": 0},
        {"value": -5},    # out of range
        {"value": 100},
    ])

    range_rule = QualityRule(
        rule_id="value-range",
        rule_type=QualityRuleType.RANGE,
        description="value must be in [0, 100]",
        field_name="value",
        min_value=0.0,
        max_value=100.0,
    )

    validator = DataQualityValidator()
    validator.add_rule(range_rule)
    reports = validator.validate(records)

    assert len(reports) == 1
    report = reports[0]

    print(f"Rule ID             : {report.rule_id}")
    print(f"Total records       : {report.total}")
    print(f"Passed              : {report.passed}")
    print(f"Failed              : {report.failed}")
    print(f"Pass rate           : {report.pass_rate:.0%}")
    print(f"Failed record IDs   : {report.failed_record_ids}")
    print(f"all_passed property : {report.all_passed}")

    assert report.total == 5
    assert report.passed == 3, f"Expected 3 passed, got {report.passed}"
    assert report.failed == 2, f"Expected 2 failed, got {report.failed}"
    # rec-001 (101) and rec-003 (-5) should fail
    assert "rec-001" in report.failed_record_ids
    assert "rec-003" in report.failed_record_ids
    assert report.all_passed is False
    print("PASS")


def _print_metrics(metrics: PipelineMetrics) -> None:
    """Pretty-print the PipelineMetrics from the happy-path run."""
    _separator("Pipeline Metrics Summary (Happy Path Run)")

    print(f"Run ID              : {metrics.run_id}")
    print(f"Started at          : {metrics.started_at:.3f}")
    print(f"Completed at        : {metrics.completed_at:.3f}")
    print(f"Total duration (s)  : {metrics.total_duration:.6f}")
    print(f"Records extracted   : {metrics.records_extracted}")
    print(f"Records transformed : {metrics.records_transformed}")
    print(f"Records loaded      : {metrics.records_loaded}")
    print(f"Records failed      : {metrics.records_failed}")
    print(f"Success rate        : {metrics.success_rate:.0%}")
    print(f"Quality reports     : {len(metrics.quality_reports)}")
    print("Stage durations:")
    for stage_id, duration in metrics.stage_durations.items():
        print(f"  {stage_id:20s}: {duration:.6f}s")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Data Pipeline Patterns — Smoke Test Suite")
    print("Patterns: Checkpointing | Quality Gates | ETL Stages | Metrics")

    happy_metrics = _run_happy_path()
    _run_checkpoint_resume()
    _run_quality_gate_failure()
    _run_quality_report()
    _print_metrics(happy_metrics)

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
