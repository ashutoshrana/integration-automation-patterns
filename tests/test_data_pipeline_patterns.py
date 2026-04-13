"""
Tests for 33_data_pipeline_patterns.py

Covers all five patterns (BatchProcessor, StreamProcessor, ETLPipeline,
DataLineage, DataQualityChecker) with 80 test cases using only the standard
library.  No external dependencies required.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import types
from pathlib import Path

import pytest

_MOD_PATH = Path(__file__).parent.parent / "examples" / "33_data_pipeline_patterns.py"


def _load_module():
    module_name = "data_pipeline_patterns_33"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, _MOD_PATH)
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ===========================================================================
# BatchProcessor
# ===========================================================================


class TestBatchProcessor:
    # --- basic process ---

    def test_process_single_full_batch(self, mod):
        proc = mod.BatchProcessor(3, lambda b: [x * 2 for x in b])
        assert proc.process([1, 2, 3]) == [2, 4, 6]

    def test_process_multiple_full_batches(self, mod):
        proc = mod.BatchProcessor(2, lambda b: [x + 10 for x in b])
        assert proc.process([1, 2, 3, 4]) == [11, 12, 13, 14]

    def test_process_partial_last_batch(self, mod):
        proc = mod.BatchProcessor(3, lambda b: b)
        result = proc.process([1, 2, 3, 4, 5])
        assert result == [1, 2, 3, 4, 5]

    def test_process_empty_input_returns_empty(self, mod):
        proc = mod.BatchProcessor(3, lambda b: b)
        assert proc.process([]) == []

    def test_process_batch_size_one(self, mod):
        proc = mod.BatchProcessor(1, lambda b: [b[0] ** 2])
        assert proc.process([2, 3, 4]) == [4, 9, 16]

    def test_process_batch_size_larger_than_input(self, mod):
        proc = mod.BatchProcessor(100, lambda b: b)
        assert proc.process([1, 2, 3]) == [1, 2, 3]

    # --- stats accumulation ---

    def test_stats_after_process(self, mod):
        proc = mod.BatchProcessor(2, lambda b: b)
        proc.process([1, 2, 3, 4, 5])
        s = proc.stats()
        assert s["total_processed"] == 5
        assert s["total_batches"] == 3
        assert s["failed_batches"] == 0

    def test_stats_accumulate_across_calls(self, mod):
        proc = mod.BatchProcessor(2, lambda b: b)
        proc.process([1, 2])
        proc.process([3, 4])
        s = proc.stats()
        assert s["total_processed"] == 4
        assert s["total_batches"] == 2

    def test_stats_initial_all_zero(self, mod):
        proc = mod.BatchProcessor(5, lambda b: b)
        s = proc.stats()
        assert s == {"total_processed": 0, "total_batches": 0, "failed_batches": 0}

    # --- process_with_retry success ---

    def test_retry_succeeds_immediately(self, mod):
        proc = mod.BatchProcessor(3, lambda b: [x + 1 for x in b])
        ok, fail = proc.process_with_retry([10, 20, 30])
        assert ok == [11, 21, 31]
        assert fail == []

    def test_retry_succeeds_on_third_attempt(self, mod):
        counts: dict[int, int] = {}

        def flaky(batch):
            counts[0] = counts.get(0, 0) + 1
            if counts[0] < 3:
                raise RuntimeError("oops")
            return batch

        proc = mod.BatchProcessor(3, flaky)
        ok, fail = proc.process_with_retry([1, 2, 3], max_retries=3)
        assert ok == [1, 2, 3]
        assert fail == []

    def test_retry_fails_all_retries(self, mod):
        proc = mod.BatchProcessor(
            2,
            lambda b: (_ for _ in ()).throw(ValueError("bad")),
        )
        ok, fail = proc.process_with_retry([1, 2, 3, 4], max_retries=2)
        assert ok == []
        assert len(fail) == 2

    def test_retry_partial_success(self, mod):
        """First batch succeeds, second always fails."""
        call_count = [0]

        def mixed(batch):
            call_count[0] += 1
            if batch[0] > 2:
                raise RuntimeError("fail second batch")
            return batch

        proc = mod.BatchProcessor(2, mixed)
        ok, fail = proc.process_with_retry([1, 2, 3, 4], max_retries=2)
        assert ok == [1, 2]
        assert fail == [[3, 4]]

    def test_retry_empty_input_returns_empty(self, mod):
        proc = mod.BatchProcessor(3, lambda b: b)
        ok, fail = proc.process_with_retry([])
        assert ok == []
        assert fail == []

    def test_retry_failed_batches_counted_in_stats(self, mod):
        proc = mod.BatchProcessor(
            2,
            lambda b: (_ for _ in ()).throw(RuntimeError()),
        )
        proc.process_with_retry([1, 2, 3, 4], max_retries=1)
        s = proc.stats()
        assert s["failed_batches"] == 2

    # --- thread safety ---

    def test_stats_thread_safe_concurrent_process(self, mod):
        proc = mod.BatchProcessor(1, lambda b: b)
        errors: list = []

        def worker():
            try:
                proc.process(list(range(10)))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        s = proc.stats()
        assert s["total_processed"] == 50


# ===========================================================================
# StreamProcessor
# ===========================================================================


class TestStreamProcessor:
    # --- tumbling windows ---

    def test_tumbling_windows_even_split(self, mod):
        sp = mod.StreamProcessor(window_size=2)
        assert sp.windows([1, 2, 3, 4]) == [[1, 2], [3, 4]]

    def test_tumbling_windows_partial_last_excluded(self, mod):
        sp = mod.StreamProcessor(window_size=3)
        # 7 records → 2 full windows of 3, last record dropped
        assert sp.windows([1, 2, 3, 4, 5, 6, 7]) == [[1, 2, 3], [4, 5, 6]]

    def test_tumbling_windows_exact_fit(self, mod):
        sp = mod.StreamProcessor(window_size=3)
        assert sp.windows([1, 2, 3, 4, 5, 6]) == [[1, 2, 3], [4, 5, 6]]

    # --- sliding windows ---

    def test_sliding_windows_step_1(self, mod):
        sp = mod.StreamProcessor(window_size=3, step_size=1)
        assert sp.windows([1, 2, 3, 4]) == [[1, 2, 3], [2, 3, 4]]

    def test_sliding_windows_step_2(self, mod):
        sp = mod.StreamProcessor(window_size=4, step_size=2)
        assert sp.windows([1, 2, 3, 4, 5, 6]) == [[1, 2, 3, 4], [3, 4, 5, 6]]

    def test_sliding_windows_produces_more_than_tumbling(self, mod):
        data = list(range(6))
        tumbling = mod.StreamProcessor(window_size=3)
        sliding = mod.StreamProcessor(window_size=3, step_size=1)
        assert len(sliding.windows(data)) > len(tumbling.windows(data))

    # --- edge cases ---

    def test_windows_empty_records_returns_empty(self, mod):
        sp = mod.StreamProcessor(window_size=3)
        assert sp.windows([]) == []

    def test_windows_records_smaller_than_window_returns_empty(self, mod):
        sp = mod.StreamProcessor(window_size=5)
        assert sp.windows([1, 2]) == []

    def test_windows_records_equal_to_window_size(self, mod):
        sp = mod.StreamProcessor(window_size=3)
        assert sp.windows([10, 20, 30]) == [[10, 20, 30]]

    # --- process_stream ---

    def test_process_stream_applies_fn_per_window(self, mod):
        sp = mod.StreamProcessor(window_size=2)
        result = sp.process_stream([1, 2, 3, 4], sum)
        assert result == [3, 7]

    def test_process_stream_returns_empty_for_empty_input(self, mod):
        sp = mod.StreamProcessor(window_size=2)
        assert sp.process_stream([], sum) == []

    # --- aggregate ---

    def test_aggregate_sum_of_sums(self, mod):
        sp = mod.StreamProcessor(window_size=2)
        assert sp.aggregate([1, 2, 3, 4], sum) == 10  # [3, 7] → 10

    def test_aggregate_with_max(self, mod):
        sp = mod.StreamProcessor(window_size=2)
        # windows: [1,2],[3,4] → max per window [2,4] → max([2,4]) = 4
        assert sp.aggregate([1, 2, 3, 4], max) == 4


# ===========================================================================
# ETLPipeline
# ===========================================================================


class TestETLPipeline:
    # --- basic run ---

    def test_run_returns_correct_counts(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2, 3]).load(lambda _: None)
        result = pipeline.run()
        assert result["extracted"] == 3
        assert result["transformed"] == 3
        assert result["loaded"] == 3

    def test_run_transform_changes_record_count(self, mod):
        pipeline = (
            mod.ETLPipeline()
            .extract(lambda: [1, 2, 3, 4])
            .transform(lambda r: [x for x in r if x % 2 == 0])
            .load(lambda _: None)
        )
        result = pipeline.run()
        assert result["extracted"] == 4
        assert result["transformed"] == 2
        assert result["loaded"] == 2

    def test_run_chained_transforms_applied_in_order(self, mod):
        sink: list = []
        pipeline = (
            mod.ETLPipeline()
            .extract(lambda: [1, 2, 3])
            .transform(lambda r: [x * 2 for x in r])
            .transform(lambda r: [x + 1 for x in r])
            .load(sink.extend)
        )
        pipeline.run()
        assert sink == [3, 5, 7]

    def test_run_without_load_fn_loads_zero(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2])
        result = pipeline.run()
        assert result["loaded"] == 0

    def test_run_empty_extract_counts_zero(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: []).load(lambda _: None)
        result = pipeline.run()
        assert result["extracted"] == 0
        assert result["transformed"] == 0
        assert result["loaded"] == 0

    def test_run_duration_ms_is_non_negative(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: list(range(1000)))
        result = pipeline.run()
        assert result["duration_ms"] >= 0

    def test_run_no_extract_fn_extracts_zero(self, mod):
        pipeline = mod.ETLPipeline().load(lambda _: None)
        result = pipeline.run()
        assert result["extracted"] == 0

    # --- dry run ---

    def test_run_dry_does_not_call_load(self, mod):
        loaded: list = []
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2, 3]).load(loaded.extend)
        pipeline.run_dry()
        assert loaded == []

    def test_run_dry_loaded_is_zero(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2, 3]).load(lambda _: None)
        result = pipeline.run_dry()
        assert result["loaded"] == 0

    def test_run_dry_extracted_and_transformed_correct(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: [10, 20]).transform(lambda r: r + [30])
        result = pipeline.run_dry()
        assert result["extracted"] == 2
        assert result["transformed"] == 3

    # --- reset ---

    def test_reset_clears_all_stages(self, mod):
        sink: list = []
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2]).transform(lambda r: [x * 10 for x in r]).load(sink.extend)
        pipeline.reset()
        pipeline.extract(lambda: ["a"]).load(sink.extend)
        pipeline.run()
        assert sink == ["a"]

    def test_reset_returns_self_for_chaining(self, mod):
        pipeline = mod.ETLPipeline()
        result = pipeline.reset()
        assert result is pipeline

    def test_run_after_reset_and_reconfigure(self, mod):
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2, 3])
        pipeline.run()
        pipeline.reset().extract(lambda: ["x", "y"]).load(lambda _: None)
        result = pipeline.run()
        assert result["extracted"] == 2


# ===========================================================================
# DataLineage
# ===========================================================================


class TestDataLineage:
    # --- add_node ---

    def test_add_node_success(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("n1", "Source", "source"))
        d = lin.to_dict()
        assert len(d["nodes"]) == 1

    def test_add_node_duplicate_raises_value_error(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("n1", "Source", "source"))
        with pytest.raises(ValueError):
            lin.add_node(mod.LineageNode("n1", "Other", "transform"))

    def test_add_node_metadata_stored(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("n1", "S", "source", {"key": "val"}))
        d = lin.to_dict()
        assert d["nodes"][0]["metadata"] == {"key": "val"}

    # --- add_edge ---

    def test_add_edge_success(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        lin.add_node(mod.LineageNode("b", "B", "sink"))
        lin.add_edge(mod.LineageEdge("a", "b", "copy"))
        d = lin.to_dict()
        assert len(d["edges"]) == 1

    def test_add_edge_unknown_from_raises_value_error(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("b", "B", "sink"))
        with pytest.raises(ValueError):
            lin.add_edge(mod.LineageEdge("unknown", "b"))

    def test_add_edge_unknown_to_raises_value_error(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        with pytest.raises(ValueError):
            lin.add_edge(mod.LineageEdge("a", "unknown"))

    # --- upstream ---

    def test_upstream_direct_predecessor(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        lin.add_node(mod.LineageNode("b", "B", "transform"))
        lin.add_edge(mod.LineageEdge("a", "b"))
        assert lin.upstream("b") == ["a"]

    def test_upstream_multiple_predecessors(self, mod):
        lin = mod.DataLineage()
        for nid in ("a", "b", "c"):
            lin.add_node(mod.LineageNode(nid, nid, "source"))
        lin.add_edge(mod.LineageEdge("a", "c"))
        lin.add_edge(mod.LineageEdge("b", "c"))
        assert set(lin.upstream("c")) == {"a", "b"}

    def test_upstream_no_predecessors_returns_empty(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        assert lin.upstream("a") == []

    # --- downstream ---

    def test_downstream_direct_successor(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        lin.add_node(mod.LineageNode("b", "B", "sink"))
        lin.add_edge(mod.LineageEdge("a", "b"))
        assert lin.downstream("a") == ["b"]

    def test_downstream_no_successors_returns_empty(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("sink", "S", "sink"))
        assert lin.downstream("sink") == []

    # --- lineage_path ---

    def test_lineage_path_direct_connection(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        lin.add_node(mod.LineageNode("b", "B", "sink"))
        lin.add_edge(mod.LineageEdge("a", "b"))
        assert lin.lineage_path("a", "b") == ["a", "b"]

    def test_lineage_path_multi_hop(self, mod):
        lin = mod.DataLineage()
        for nid in ("a", "b", "c", "d"):
            lin.add_node(mod.LineageNode(nid, nid, "transform"))
        lin.add_edge(mod.LineageEdge("a", "b"))
        lin.add_edge(mod.LineageEdge("b", "c"))
        lin.add_edge(mod.LineageEdge("c", "d"))
        assert lin.lineage_path("a", "d") == ["a", "b", "c", "d"]

    def test_lineage_path_no_path_returns_empty(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        lin.add_node(mod.LineageNode("b", "B", "sink"))
        # No edge added
        assert lin.lineage_path("a", "b") == []

    def test_lineage_path_reverse_direction_returns_empty(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        lin.add_node(mod.LineageNode("b", "B", "sink"))
        lin.add_edge(mod.LineageEdge("a", "b"))
        assert lin.lineage_path("b", "a") == []

    def test_lineage_path_same_node(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("a", "A", "source"))
        assert lin.lineage_path("a", "a") == ["a"]

    def test_lineage_path_bfs_finds_shortest(self, mod):
        """Graph has two paths; BFS should return shorter one."""
        lin = mod.DataLineage()
        for nid in ("a", "b", "c", "d"):
            lin.add_node(mod.LineageNode(nid, nid, "transform"))
        lin.add_edge(mod.LineageEdge("a", "b"))
        lin.add_edge(mod.LineageEdge("b", "d"))  # short path: a→b→d
        lin.add_edge(mod.LineageEdge("a", "c"))
        lin.add_edge(mod.LineageEdge("c", "d"))  # also 2 hops
        path = lin.lineage_path("a", "d")
        assert len(path) == 3  # shortest is 3 nodes (2 hops)
        assert path[0] == "a"
        assert path[-1] == "d"

    # --- to_dict serialisation ---

    def test_to_dict_structure(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("s", "Source", "source"))
        lin.add_node(mod.LineageNode("t", "Target", "sink"))
        lin.add_edge(mod.LineageEdge("s", "t", "passthrough"))
        d = lin.to_dict()
        assert "nodes" in d
        assert "edges" in d
        assert d["edges"][0]["transformation"] == "passthrough"

    def test_to_dict_empty_graph(self, mod):
        lin = mod.DataLineage()
        d = lin.to_dict()
        assert d == {"nodes": [], "edges": []}

    def test_to_dict_node_fields_present(self, mod):
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("x", "X", "transform", {"v": 1}))
        d = lin.to_dict()
        node = d["nodes"][0]
        assert node["node_id"] == "x"
        assert node["name"] == "X"
        assert node["node_type"] == "transform"
        assert node["metadata"] == {"v": 1}


# ===========================================================================
# DataQualityChecker
# ===========================================================================


class TestDataQualityChecker:
    # --- add_rule + check ---

    def test_check_single_rule_pass(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("positive", lambda x: x > 0)
        assert checker.check(1) == {"positive": True}

    def test_check_single_rule_fail(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("positive", lambda x: x > 0)
        assert checker.check(-1) == {"positive": False}

    def test_check_multiple_rules_all_pass(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("r1", lambda x: x > 0)
        checker.add_rule("r2", lambda x: x < 100)
        result = checker.check(50)
        assert result == {"r1": True, "r2": True}

    def test_check_multiple_rules_partial_fail(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("r1", lambda x: x > 0)
        checker.add_rule("r2", lambda x: x < 10)
        result = checker.check(50)
        assert result == {"r1": True, "r2": False}

    def test_check_no_rules_returns_empty_dict(self, mod):
        checker = mod.DataQualityChecker()
        assert checker.check(42) == {}

    # --- check_batch ---

    def test_check_batch_counts(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x >= 0)
        stats = checker.check_batch([-1, 0, 1, 2])
        assert stats["total"] == 4
        assert stats["passed"] == 3
        assert stats["failed"] == 1

    def test_check_batch_failure_rate(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x >= 0)
        stats = checker.check_batch([-1, -2, 1, 2])
        assert stats["failure_rate"] == pytest.approx(0.5)

    def test_check_batch_violations_dict(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("r1", lambda x: x > 0)
        checker.add_rule("r2", lambda x: x < 5)
        stats = checker.check_batch([-1, 3, 10])
        # -1 fails r1; 10 fails r2; 3 passes all
        assert stats["violations"]["r1"] == 1
        assert stats["violations"]["r2"] == 1

    def test_check_batch_empty_input(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("r", lambda x: x > 0)
        stats = checker.check_batch([])
        assert stats["total"] == 0
        assert stats["passed"] == 0
        assert stats["failed"] == 0
        assert stats["failure_rate"] == 0.0

    def test_check_batch_all_pass(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x > 0)
        stats = checker.check_batch([1, 2, 3])
        assert stats["passed"] == 3
        assert stats["failed"] == 0
        assert stats["failure_rate"] == pytest.approx(0.0)

    # --- valid_only ---

    def test_valid_only_filters_correctly(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x > 0)
        checker.add_rule("lt10", lambda x: x < 10)
        result = checker.valid_only([-1, 0, 5, 10, 7])
        assert result == [5, 7]

    def test_valid_only_all_valid(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x > 0)
        assert checker.valid_only([1, 2, 3]) == [1, 2, 3]

    def test_valid_only_none_valid(self, mod):
        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x > 0)
        assert checker.valid_only([-1, -2]) == []

    def test_valid_only_no_rules_returns_all(self, mod):
        checker = mod.DataQualityChecker()
        records = [1, 2, 3]
        assert checker.valid_only(records) == records


# ===========================================================================
# Integration tests
# ===========================================================================


class TestIntegration:
    def test_etl_pipeline_using_batch_processor_as_transform(self, mod):
        """ETL pipeline delegates its transform stage to a BatchProcessor."""
        proc = mod.BatchProcessor(2, lambda b: [x * 3 for x in b])
        sink: list = []
        pipeline = mod.ETLPipeline().extract(lambda: [1, 2, 3, 4]).transform(proc.process).load(sink.extend)
        result = pipeline.run()
        assert result["extracted"] == 4
        assert result["loaded"] == 4
        assert sink == [3, 6, 9, 12]
        assert proc.stats()["total_processed"] == 4

    def test_etl_pipeline_with_quality_filter_transform(self, mod):
        """Transform stage uses DataQualityChecker.valid_only to filter records."""
        checker = mod.DataQualityChecker()
        checker.add_rule("non_neg", lambda x: x >= 0)
        sink: list = []
        pipeline = mod.ETLPipeline().extract(lambda: [-1, 0, 1, -2, 2]).transform(checker.valid_only).load(sink.extend)
        result = pipeline.run()
        assert result["extracted"] == 5
        assert result["transformed"] == 3
        assert sink == [0, 1, 2]

    def test_lineage_tracks_etl_pipeline_stages(self, mod):
        """DataLineage can model an ETL pipeline graph."""
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("extract", "DB Source", "source"))
        lin.add_node(mod.LineageNode("transform", "Clean & Filter", "transform"))
        lin.add_node(mod.LineageNode("load", "Data Warehouse", "sink"))
        lin.add_edge(mod.LineageEdge("extract", "transform", "null_drop"))
        lin.add_edge(mod.LineageEdge("transform", "load", "bulk_insert"))
        assert lin.lineage_path("extract", "load") == [
            "extract",
            "transform",
            "load",
        ]

    def test_batch_processor_and_stream_processor_chained(self, mod):
        """Batch-process records then apply a sliding window aggregate."""
        proc = mod.BatchProcessor(2, lambda b: [x**2 for x in b])
        squared = proc.process([1, 2, 3, 4])  # [1, 4, 9, 16]
        sp = mod.StreamProcessor(window_size=2)
        window_sums = sp.process_stream(squared, sum)
        assert window_sums == [5, 25]

    def test_quality_checker_combined_with_batch_processor(self, mod):
        """DataQualityChecker validates records before BatchProcessor processes them."""
        checker = mod.DataQualityChecker()
        checker.add_rule("even", lambda x: x % 2 == 0)
        valid = checker.valid_only([1, 2, 3, 4, 5, 6])  # [2, 4, 6]
        proc = mod.BatchProcessor(2, lambda b: [x // 2 for x in b])
        result = proc.process(valid)
        assert result == [1, 2, 3]

    def test_full_pipeline_with_lineage_and_quality(self, mod):
        """Full end-to-end: extract → quality filter → batch transform → load,
        with lineage graph tracking the stages."""
        lin = mod.DataLineage()
        lin.add_node(mod.LineageNode("raw", "Raw", "source"))
        lin.add_node(mod.LineageNode("valid", "Valid Only", "transform"))
        lin.add_node(mod.LineageNode("scaled", "Scaled", "transform"))
        lin.add_node(mod.LineageNode("out", "Output", "sink"))
        lin.add_edge(mod.LineageEdge("raw", "valid"))
        lin.add_edge(mod.LineageEdge("valid", "scaled"))
        lin.add_edge(mod.LineageEdge("scaled", "out"))

        checker = mod.DataQualityChecker()
        checker.add_rule("pos", lambda x: x > 0)
        proc = mod.BatchProcessor(2, lambda b: [x * 10 for x in b])
        sink: list = []

        pipeline = (
            mod.ETLPipeline()
            .extract(lambda: [-1, 1, 2, 3])
            .transform(checker.valid_only)
            .transform(proc.process)
            .load(sink.extend)
        )
        result = pipeline.run()
        assert result["extracted"] == 4
        assert result["transformed"] == 3
        assert sink == [10, 20, 30]
        assert lin.lineage_path("raw", "out") == [
            "raw",
            "valid",
            "scaled",
            "out",
        ]
