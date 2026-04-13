"""
33_data_pipeline_patterns.py — Data Pipeline Patterns

Pure-Python implementation of core data pipeline primitives using only the
standard library (``dataclasses``, ``threading``, ``time``, ``collections``,
``typing``).  No external data-processing libraries are required.

    Pattern 1 — BatchProcessor (fixed-size batch processing):
                Splits a record list into fixed-size batches and applies a
                caller-supplied transformation function to each batch.
                Supports retry with configurable max-retries and exposes
                thread-safe statistics for total processed / failed batches.

    Pattern 2 — StreamProcessor (sliding/tumbling window processing):
                Generates tumbling (non-overlapping) or sliding (overlapping)
                windows over a record list.  Applies a function to each window
                and supports reducing all window results to a single aggregate.

    Pattern 3 — ETLPipeline (composable extract-transform-load):
                Fluent builder for registering one extract function, N ordered
                transform functions, and one load function.  ``run()`` executes
                the full pipeline; ``run_dry()`` skips the load step; both
                return timing and count metrics.

    Pattern 4 — DataLineage (data provenance graph):
                Directed graph of ``LineageNode`` and ``LineageEdge`` objects
                tracking how data flows from sources through transforms to
                sinks.  Supports upstream/downstream queries and BFS shortest-
                path computation.

    Pattern 5 — DataQualityChecker (rule-based record validation):
                Registers named validation callables and evaluates them against
                individual records or batches.  Reports per-rule violation
                counts and filters records to those passing all rules.

No external dependencies required.

Run:
    python examples/33_data_pipeline_patterns.py
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# ===========================================================================
# Pattern 1 — BatchProcessor
# ===========================================================================


class BatchProcessor:
    """Processes records in fixed-size batches using a caller-supplied function.

    Records are split into consecutive batches of :attr:`batch_size`; the
    final batch may be smaller if ``len(records)`` is not evenly divisible.
    All statistics are updated atomically via an internal ``threading.Lock``.

    Example::

        processor = BatchProcessor(batch_size=3, processor_fn=lambda b: [x * 2 for x in b])
        results = processor.process([1, 2, 3, 4, 5])
        print(results)   # [2, 4, 6, 8, 10]
        print(processor.stats())
        # {'total_processed': 5, 'total_batches': 2, 'failed_batches': 0}
    """

    def __init__(
        self,
        batch_size: int,
        processor_fn: Callable[[list], list],
    ) -> None:
        """Initialise a :class:`BatchProcessor`.

        Args:
            batch_size:    Number of records per batch (must be >= 1).
            processor_fn:  Callable that receives a list of records and returns
                           a list of results of the same or different length.
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1; got {batch_size}")
        self.batch_size = batch_size
        self.processor_fn = processor_fn

        self._total_processed: int = 0
        self._total_batches: int = 0
        self._failed_batches: int = 0
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _split(self, records: list) -> list[list]:
        """Return *records* split into batches of :attr:`batch_size`."""
        return [
            records[i : i + self.batch_size]
            for i in range(0, max(len(records), 1), self.batch_size)
            if records[i : i + self.batch_size]
        ]

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process(self, records: list) -> list:
        """Process *records* in batches; raise on any ``processor_fn`` failure.

        Concatenates each batch's result list into a single output list.
        Statistics are updated for every batch executed.

        Args:
            records: Input records to process.

        Returns:
            Concatenated list of all batch results.

        Raises:
            Exception: Re-raises any exception thrown by ``processor_fn``.
        """
        if not records:
            return []
        batches = self._split(records)
        output: list = []
        for batch in batches:
            result = self.processor_fn(batch)
            output.extend(result)
            with self._lock:
                self._total_processed += len(batch)
                self._total_batches += 1
        return output

    def process_with_retry(
        self,
        records: list,
        max_retries: int = 3,
    ) -> tuple[list, list]:
        """Process *records* in batches, retrying failed batches.

        If ``processor_fn`` raises for a batch, the batch is retried up to
        *max_retries* times.  Batches that still fail after all retries are
        collected in the second element of the returned tuple.

        Args:
            records:     Input records to process.
            max_retries: Maximum number of attempts per batch (default ``3``).

        Returns:
            Tuple ``(successful_results, failed_batches)`` where
            ``successful_results`` is the concatenated output from all
            successful batches and ``failed_batches`` is a list of the raw
            input batches that ultimately failed.
        """
        if not records:
            return [], []
        batches = self._split(records)
        successful: list = []
        failed_batches: list[list] = []

        for batch in batches:
            last_exc: Exception | None = None
            succeeded = False
            for _attempt in range(max_retries):
                try:
                    result = self.processor_fn(batch)
                    successful.extend(result)
                    with self._lock:
                        self._total_processed += len(batch)
                        self._total_batches += 1
                    succeeded = True
                    break
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc  # noqa: F841
            if not succeeded:
                failed_batches.append(batch)
                with self._lock:
                    self._total_batches += 1
                    self._failed_batches += 1

        return successful, failed_batches

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return accumulated processing statistics.

        Returns:
            Dict with keys ``"total_processed"`` (int), ``"total_batches"``
            (int), and ``"failed_batches"`` (int).
        """
        with self._lock:
            return {
                "total_processed": self._total_processed,
                "total_batches": self._total_batches,
                "failed_batches": self._failed_batches,
            }


# ===========================================================================
# Pattern 2 — StreamProcessor
# ===========================================================================


class StreamProcessor:
    """Processes records as a sliding or tumbling window stream.

    A *tumbling* window advances by its full size after each step (no overlap).
    A *sliding* window advances by ``step_size < window_size``, so consecutive
    windows share records.

    Example::

        sp = StreamProcessor(window_size=3)          # tumbling
        print(sp.windows([1, 2, 3, 4, 5, 6]))
        # [[1, 2, 3], [4, 5, 6]]

        sp2 = StreamProcessor(window_size=3, step_size=1)  # sliding
        print(sp2.windows([1, 2, 3, 4]))
        # [[1, 2, 3], [2, 3, 4]]
    """

    def __init__(
        self,
        window_size: int,
        step_size: int | None = None,
    ) -> None:
        """Initialise a :class:`StreamProcessor`.

        Args:
            window_size: Number of records in each window (must be >= 1).
            step_size:   How far to advance between windows.  Defaults to
                         ``window_size`` (tumbling).  Set to a value smaller
                         than ``window_size`` for a sliding window.
        """
        if window_size < 1:
            raise ValueError(f"window_size must be >= 1; got {window_size}")
        self.window_size = window_size
        self.step_size: int = window_size if step_size is None else step_size
        if self.step_size < 1:
            raise ValueError(f"step_size must be >= 1; got {self.step_size}")

    # ------------------------------------------------------------------
    # Window generation
    # ------------------------------------------------------------------

    def windows(self, records: list) -> list[list]:
        """Return all windows over *records*.

        Stops when the next window would exceed the end of the list.

        Args:
            records: Input record list.

        Returns:
            List of windows, each a sub-list of length :attr:`window_size`.
            Returns ``[]`` if ``len(records) < window_size``.
        """
        result: list[list] = []
        i = 0
        while i + self.window_size <= len(records):
            result.append(records[i : i + self.window_size])
            i += self.step_size
        return result

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process_stream(self, records: list, fn: Callable[[list], Any]) -> list:
        """Apply *fn* to each window and return the list of results.

        Args:
            records: Input record list.
            fn:      Callable applied to each window; may return any value.

        Returns:
            List of results, one per window.
        """
        return [fn(w) for w in self.windows(records)]

    def aggregate(self, records: list, fn: Callable[[list], Any]) -> Any:
        """Apply *fn* to the list of window results and return the aggregate.

        Example: ``aggregate(records, fn=sum)`` computes the sum of every
        window's result.

        Args:
            records: Input record list.
            fn:      Reduction function applied to the list of window results.

        Returns:
            Single aggregated value.
        """
        window_results = self.process_stream(records, fn)
        return fn(window_results)


# ===========================================================================
# Pattern 3 — ETLPipeline
# ===========================================================================


class ETLPipeline:
    """Composable extract-transform-load pipeline with fluent builder API.

    Registers exactly one extract function, zero or more ordered transform
    functions, and optionally one load function.  ``run()`` executes all
    stages; ``run_dry()`` skips the load.

    Example::

        pipeline = (
            ETLPipeline()
            .extract(lambda: [1, 2, 3])
            .transform(lambda recs: [r * 2 for r in recs])
            .load(lambda recs: print("Loaded:", recs))
        )
        result = pipeline.run()
        print(result)
        # {'extracted': 3, 'transformed': 3, 'loaded': 3, 'duration_ms': ...}
    """

    def __init__(self) -> None:
        self._extract_fn: Callable[[], list] | None = None
        self._transform_fns: list[Callable[[list], list]] = []
        self._load_fn: Callable[[list], None] | None = None

    # ------------------------------------------------------------------
    # Builder
    # ------------------------------------------------------------------

    def extract(self, fn: Callable[[], list]) -> ETLPipeline:
        """Register the source/extract function.

        Args:
            fn: Zero-argument callable returning a list of records.

        Returns:
            *self* for method chaining.
        """
        self._extract_fn = fn
        return self

    def transform(self, fn: Callable[[list], list]) -> ETLPipeline:
        """Append a transformation stage.

        Transforms are applied in registration order.

        Args:
            fn: Callable that receives a list and returns a (transformed) list.

        Returns:
            *self* for method chaining.
        """
        self._transform_fns.append(fn)
        return self

    def load(self, fn: Callable[[list], None]) -> ETLPipeline:
        """Register the sink/load function.

        Args:
            fn: Callable that receives the final transformed list; return
                value is ignored.

        Returns:
            *self* for method chaining.
        """
        self._load_fn = fn
        return self

    def reset(self) -> ETLPipeline:
        """Clear all registered functions and return *self*.

        Returns:
            *self* for method chaining.
        """
        self._extract_fn = None
        self._transform_fns = []
        self._load_fn = None
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _execute(self, *, dry: bool) -> dict:
        """Internal pipeline executor.

        Args:
            dry: When ``True``, skip the load stage.

        Returns:
            Metrics dict with ``extracted``, ``transformed``, ``loaded``,
            and ``duration_ms``.
        """
        start = time.perf_counter()

        # Extract
        records: list = self._extract_fn() if self._extract_fn is not None else []
        extracted_count = len(records)

        # Transform
        for fn in self._transform_fns:
            records = fn(records)
        transformed_count = len(records)

        # Load
        loaded_count = 0
        if not dry and self._load_fn is not None:
            self._load_fn(records)
            loaded_count = transformed_count

        duration_ms = (time.perf_counter() - start) * 1000.0
        return {
            "extracted": extracted_count,
            "transformed": transformed_count,
            "loaded": loaded_count,
            "duration_ms": duration_ms,
        }

    def run(self) -> dict:
        """Execute extract → transform(s) → load and return metrics.

        Returns:
            Dict with integer counts ``"extracted"``, ``"transformed"``,
            ``"loaded"`` and float ``"duration_ms"``.
        """
        return self._execute(dry=False)

    def run_dry(self) -> dict:
        """Execute extract → transform(s) but skip load; return metrics.

        Returns:
            Same structure as :meth:`run` with ``"loaded": 0``.
        """
        return self._execute(dry=True)


# ===========================================================================
# Pattern 4 — DataLineage
# ===========================================================================


@dataclass
class LineageNode:
    """A node in the data lineage graph.

    Attributes:
        node_id:   Unique identifier for this node.
        name:      Human-readable display name.
        node_type: One of ``"source"``, ``"transform"``, or ``"sink"``.
        metadata:  Arbitrary key-value metadata.
    """

    node_id: str
    name: str
    node_type: str  # "source" | "transform" | "sink"
    metadata: dict = field(default_factory=dict)


@dataclass
class LineageEdge:
    """A directed edge representing data flow between two nodes.

    Attributes:
        from_node:      Source node ID.
        to_node:        Destination node ID.
        transformation: Optional description of the transformation applied.
    """

    from_node: str
    to_node: str
    transformation: str = ""


class DataLineage:
    """Directed graph tracking data provenance and transformation paths.

    Nodes represent data sources, transforms, and sinks.  Edges represent
    directed data flow.  Supports upstream / downstream queries and BFS
    shortest-path computation.

    Example::

        lineage = DataLineage()
        lineage.add_node(LineageNode("src", "DB Table", "source"))
        lineage.add_node(LineageNode("agg", "Aggregation", "transform"))
        lineage.add_node(LineageNode("dst", "Data Warehouse", "sink"))
        lineage.add_edge(LineageEdge("src", "agg"))
        lineage.add_edge(LineageEdge("agg", "dst"))
        print(lineage.lineage_path("src", "dst"))  # ['src', 'agg', 'dst']
    """

    def __init__(self) -> None:
        self._nodes: dict[str, LineageNode] = {}
        self._edges: list[LineageEdge] = []

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def add_node(self, node: LineageNode) -> None:
        """Register *node* in the lineage graph.

        Args:
            node: :class:`LineageNode` to add.

        Raises:
            ValueError: If ``node.node_id`` is already registered.
        """
        if node.node_id in self._nodes:
            raise ValueError(f"Node ID {node.node_id!r} already exists")
        self._nodes[node.node_id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        """Register a directed *edge* in the lineage graph.

        Args:
            edge: :class:`LineageEdge` to add.

        Raises:
            ValueError: If either ``from_node`` or ``to_node`` is unknown.
        """
        if edge.from_node not in self._nodes:
            raise ValueError(f"Unknown from_node {edge.from_node!r}")
        if edge.to_node not in self._nodes:
            raise ValueError(f"Unknown to_node {edge.to_node!r}")
        self._edges.append(edge)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def upstream(self, node_id: str) -> list[str]:
        """Return direct predecessors (nodes that flow INTO *node_id*).

        Args:
            node_id: Target node ID.

        Returns:
            List of node IDs whose edges point to *node_id*.
        """
        return [e.from_node for e in self._edges if e.to_node == node_id]

    def downstream(self, node_id: str) -> list[str]:
        """Return direct successors (nodes that flow OUT of *node_id*).

        Args:
            node_id: Source node ID.

        Returns:
            List of node IDs that *node_id* flows into.
        """
        return [e.to_node for e in self._edges if e.from_node == node_id]

    def lineage_path(self, from_id: str, to_id: str) -> list[str]:
        """Return the shortest node-ID path from *from_id* to *to_id* via BFS.

        Args:
            from_id: Starting node ID.
            to_id:   Destination node ID.

        Returns:
            List of node IDs representing the shortest path (inclusive of
            both endpoints).  Returns ``[]`` if no path exists.
        """
        if from_id == to_id:
            return [from_id]
        visited: set[str] = {from_id}
        queue: deque[list[str]] = deque([[from_id]])
        while queue:
            path = queue.popleft()
            current = path[-1]
            for neighbour in self.downstream(current):
                if neighbour == to_id:
                    return [*path, neighbour]
                if neighbour not in visited:
                    visited.add(neighbour)
                    queue.append([*path, neighbour])
        return []

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialise the lineage graph to a JSON-compatible dict.

        Returns:
            Dict with keys ``"nodes"`` (list of node dicts) and ``"edges"``
            (list of edge dicts).
        """
        return {
            "nodes": [
                {
                    "node_id": n.node_id,
                    "name": n.name,
                    "node_type": n.node_type,
                    "metadata": n.metadata,
                }
                for n in self._nodes.values()
            ],
            "edges": [
                {
                    "from_node": e.from_node,
                    "to_node": e.to_node,
                    "transformation": e.transformation,
                }
                for e in self._edges
            ],
        }


# ===========================================================================
# Pattern 5 — DataQualityChecker
# ===========================================================================


class DataQualityChecker:
    """Validates records against a set of named rules.

    Rules are registered with :meth:`add_rule` and evaluated via
    :meth:`check` (single record) or :meth:`check_batch` (many records).

    Example::

        checker = DataQualityChecker()
        checker.add_rule("non_negative", lambda x: x >= 0)
        checker.add_rule("under_1000",  lambda x: x < 1000)
        print(checker.check(42))       # {'non_negative': True, 'under_1000': True}
        print(checker.check(-1))       # {'non_negative': False, 'under_1000': True}
    """

    def __init__(self) -> None:
        # name → (fn, description)
        self._rules: dict[str, tuple[Callable[[Any], bool], str]] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_rule(
        self,
        name: str,
        fn: Callable[[Any], bool],
        description: str = "",
    ) -> None:
        """Register a named validation rule.

        Args:
            name:        Unique rule identifier.
            fn:          Callable that receives a record and returns ``bool``.
            description: Optional human-readable description.
        """
        self._rules[name] = (fn, description)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def check(self, record: Any) -> dict[str, bool]:
        """Run all rules on *record* and return per-rule pass/fail.

        Args:
            record: The record to validate.

        Returns:
            Dict mapping rule name → ``True`` (passed) or ``False`` (failed).
        """
        return {name: bool(fn(record)) for name, (fn, _) in self._rules.items()}

    def check_batch(self, records: list) -> dict:
        """Validate *records* against all rules and return aggregate stats.

        Args:
            records: List of records to validate.

        Returns:
            Dict with keys:

            * ``"total"``        — total number of records.
            * ``"passed"``       — records where ALL rules passed.
            * ``"failed"``       — records where at least one rule failed.
            * ``"failure_rate"`` — ``failed / total`` (0.0 if no records).
            * ``"violations"``   — dict mapping rule name → failure count.
        """
        total = len(records)
        passed = 0
        violations: dict[str, int] = {name: 0 for name in self._rules}

        for record in records:
            result = self.check(record)
            record_passed = all(result.values())
            if record_passed:
                passed += 1
            for rule_name, ok in result.items():
                if not ok:
                    violations[rule_name] += 1

        failed = total - passed
        failure_rate = failed / total if total > 0 else 0.0
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "failure_rate": failure_rate,
            "violations": violations,
        }

    def valid_only(self, records: list) -> list:
        """Return only those records where ALL rules pass.

        Args:
            records: List of records to filter.

        Returns:
            Sub-list of *records* that satisfy every registered rule.
        """
        return [r for r in records if all(self.check(r).values())]


# ===========================================================================
# Smoke-test helpers (used only in __main__)
# ===========================================================================


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_batch_processor_demo() -> None:
    """Scenario 1: Batch processing with retry."""
    _separator("Scenario 1 — BatchProcessor: basic + retry")

    # Basic batch processing (double every value)
    processor = BatchProcessor(
        batch_size=3,
        processor_fn=lambda batch: [x * 2 for x in batch],
    )
    results = processor.process([1, 2, 3, 4, 5])
    print(f"process([1..5]) → {results}")
    assert results == [2, 4, 6, 8, 10]

    stats = processor.stats()
    print(f"stats: {stats}")
    assert stats["total_processed"] == 5
    assert stats["total_batches"] == 2
    assert stats["failed_batches"] == 0

    # Retry: first two attempts raise, third succeeds
    attempt_counts: dict[str, int] = {}

    def flaky_fn(batch: list) -> list:
        key = str(batch)
        attempt_counts[key] = attempt_counts.get(key, 0) + 1
        if attempt_counts[key] < 3:
            raise RuntimeError("transient error")
        return [x + 100 for x in batch]

    retry_proc = BatchProcessor(batch_size=2, processor_fn=flaky_fn)
    successful, failed = retry_proc.process_with_retry([10, 20], max_retries=3)
    print(f"retry successful: {successful}, failed: {failed}")
    assert successful == [110, 120]
    assert failed == []

    # Retry: always fails → ends up in failed_batches
    always_fail = BatchProcessor(
        batch_size=2,
        processor_fn=lambda b: (_ for _ in ()).throw(RuntimeError("always")),
    )
    s2, f2 = always_fail.process_with_retry([1, 2, 3, 4], max_retries=2)
    print(f"always-fail: successful={s2}, failed_batches={len(f2)}")
    assert s2 == []
    assert len(f2) == 2

    print("PASS")


def _run_stream_processor_demo() -> None:
    """Scenario 2: Tumbling vs sliding windows."""
    _separator("Scenario 2 — StreamProcessor: tumbling + sliding windows")

    data = [1, 2, 3, 4, 5, 6]

    # Tumbling (step = window)
    tumbling = StreamProcessor(window_size=2)
    wins = tumbling.windows(data)
    print(f"tumbling windows(2): {wins}")
    assert wins == [[1, 2], [3, 4], [5, 6]]

    # Sliding (step < window)
    sliding = StreamProcessor(window_size=3, step_size=1)
    wins2 = sliding.windows(data)
    print(f"sliding windows(3, step=1): {wins2}")
    assert wins2 == [[1, 2, 3], [2, 3, 4], [3, 4, 5], [4, 5, 6]]

    # process_stream: sum each window
    sums = tumbling.process_stream(data, sum)
    print(f"process_stream sum: {sums}")
    assert sums == [3, 7, 11]

    # aggregate: sum of sums
    total = tumbling.aggregate(data, sum)
    print(f"aggregate (sum of sums): {total}")
    assert total == 21

    print("PASS")


def _run_etl_pipeline_demo() -> None:
    """Scenario 3: ETL pipeline with dry run."""
    _separator("Scenario 3 — ETLPipeline: extract + transform + load")

    loaded_sink: list = []

    pipeline = (
        ETLPipeline()
        .extract(lambda: list(range(1, 6)))
        .transform(lambda recs: [r * 2 for r in recs])
        .transform(lambda recs: [r + 1 for r in recs])
        .load(loaded_sink.extend)
    )

    # Dry run — load not called
    dry_result = pipeline.run_dry()
    print(f"dry run: {dry_result}")
    assert dry_result["extracted"] == 5
    assert dry_result["transformed"] == 5
    assert dry_result["loaded"] == 0
    assert dry_result["duration_ms"] >= 0
    assert loaded_sink == []

    # Full run — load called
    result = pipeline.run()
    print(f"full run: {result}")
    assert result["extracted"] == 5
    assert result["transformed"] == 5
    assert result["loaded"] == 5
    assert loaded_sink == [3, 5, 7, 9, 11]

    # Reset and re-run with new extract
    pipeline.reset().extract(lambda: ["a", "b"]).load(lambda _: None)
    r2 = pipeline.run()
    assert r2["extracted"] == 2

    print("PASS")


def _run_data_lineage_demo() -> None:
    """Scenario 4: Lineage graph with path query."""
    _separator("Scenario 4 — DataLineage: graph + BFS path")

    lineage = DataLineage()
    lineage.add_node(LineageNode("raw", "Raw Events", "source"))
    lineage.add_node(LineageNode("clean", "Cleaned Events", "transform", {"version": "2"}))
    lineage.add_node(LineageNode("agg", "Daily Aggregates", "transform"))
    lineage.add_node(LineageNode("dw", "Data Warehouse", "sink"))

    lineage.add_edge(LineageEdge("raw", "clean", transformation="null_drop"))
    lineage.add_edge(LineageEdge("clean", "agg", transformation="group_by_day"))
    lineage.add_edge(LineageEdge("agg", "dw"))

    # Upstream / downstream
    up = lineage.upstream("agg")
    dn = lineage.downstream("clean")
    print(f"upstream('agg'): {up}")
    print(f"downstream('clean'): {dn}")
    assert up == ["clean"]
    assert dn == ["agg"]

    # BFS path
    path = lineage.lineage_path("raw", "dw")
    print(f"lineage_path('raw' → 'dw'): {path}")
    assert path == ["raw", "clean", "agg", "dw"]

    # No path
    assert lineage.lineage_path("dw", "raw") == []

    # Serialisation
    d = lineage.to_dict()
    assert len(d["nodes"]) == 4
    assert len(d["edges"]) == 3
    print(f"to_dict: {len(d['nodes'])} nodes, {len(d['edges'])} edges")

    print("PASS")


def _run_data_quality_demo() -> None:
    """Scenario 5: Quality checker on a batch of numbers."""
    _separator("Scenario 5 — DataQualityChecker: rule validation")

    checker = DataQualityChecker()
    checker.add_rule("non_negative", lambda x: x >= 0, "Value must be >= 0")
    checker.add_rule("under_100", lambda x: x < 100, "Value must be < 100")

    # Single check
    result = checker.check(42)
    print(f"check(42): {result}")
    assert result == {"non_negative": True, "under_100": True}

    result_fail = checker.check(-5)
    assert result_fail == {"non_negative": False, "under_100": True}

    # Batch check
    records = [-1, 0, 50, 99, 100, 200]
    batch = checker.check_batch(records)
    print(f"check_batch stats: {batch}")
    # -1 fails non_negative (1 failure)
    # 100, 200 fail under_100 (2 failures)
    # passed: 0, 50, 99 → 3 records pass all
    assert batch["total"] == 6
    assert batch["passed"] == 3
    assert batch["failed"] == 3
    assert batch["violations"]["non_negative"] == 1
    assert batch["violations"]["under_100"] == 2

    # valid_only
    valid = checker.valid_only(records)
    print(f"valid_only: {valid}")
    assert valid == [0, 50, 99]

    print("PASS")


# ===========================================================================
# Entry point
# ===========================================================================


if __name__ == "__main__":
    print("Data Pipeline Patterns — Smoke Test Suite")
    print("Patterns: BatchProcessor | StreamProcessor | ETLPipeline | DataLineage | DataQualityChecker")

    _run_batch_processor_demo()
    _run_stream_processor_demo()
    _run_etl_pipeline_demo()
    _run_data_lineage_demo()
    _run_data_quality_demo()

    print("\n" + "=" * 60)
    print("  All scenarios passed.")
    print("=" * 60)
