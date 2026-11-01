"""CI Performance Regression Detection Tests.

This test module is designed to run in CI to catch performance regressions.
It compares current benchmark results against baseline thresholds defined in
baselines.json.

Reference: mcp_agent_mail-tty (CI performance regression detection task)

Usage:
    # Run regression checks (uses existing benchmark results)
    pytest tests/benchmarks/test_ci_regression.py -v

    # Run with benchmarks first (generates fresh results)
    RUN_BENCHMARKS=1 pytest tests/benchmarks/ -v -m "benchmark or ci_regression"

Environment variables:
    CI_REGRESSION_STRICT: Set to "1" to fail on warnings too (default: errors only)
    CI_REGRESSION_SKIP: Set to "1" to skip regression checks entirely
"""

from __future__ import annotations

import json
import math
import os

import pytest

from . import regression
from .regression import (
    assert_no_regressions,
    assert_query_count_lte,
    check_regressions,
    load_baselines,
    render_report,
    write_report_json,
)


def _should_skip_regression() -> bool:
    """Check if regression checks should be skipped."""
    return os.getenv("CI_REGRESSION_SKIP") == "1"


def _is_strict_mode() -> bool:
    """Check if strict mode is enabled (fail on warnings)."""
    return os.getenv("CI_REGRESSION_STRICT") == "1"


@pytest.mark.ci_regression
def test_performance_regression_check():
    """Main CI regression check - fails if thresholds exceeded.

    This test:
    1. Loads baseline thresholds from baselines.json
    2. Loads latest benchmark results from tests/benchmarks/results/
    3. Compares each metric against thresholds
    4. Renders a Rich report to stdout for CI logs
    5. Writes JSON artifact for CI download
    6. Fails if any violation (error) is found
    """
    if _should_skip_regression():
        pytest.skip("Skipped by CI_REGRESSION_SKIP=1")
    if not any(regression.RESULTS_DIR.glob("*.json")):
        # No benchmark has run here (e.g. the main test step): there's nothing to judge. CI's
        # regression step runs this after the benchmarks, where a missing result is an error.
        pytest.skip("No benchmark results: run tests/benchmarks/bench_*.py first (make bench)")

    # This will raise AssertionError on violations
    assert_no_regressions()


@pytest.mark.ci_regression
def test_baselines_file_valid():
    """Validate that baselines.json is well-formed and has required fields."""
    baselines = load_baselines()

    assert "version" in baselines, "baselines.json missing 'version' field"
    assert "thresholds" in baselines, "baselines.json missing 'thresholds' field"
    assert "baselines" in baselines, "baselines.json missing 'baselines' field"

    # Check that baseline configs have required fields
    for tool, config in baselines.get("baselines", {}).items():
        assert "latency_ms" in config, f"Baseline for {tool} missing 'latency_ms'"
        assert "p95_max" in config["latency_ms"], f"Baseline for {tool} missing 'latency_ms.p95_max'"


def test_dataset_mismatch_fails_the_check():
    """Thresholds measured on one workload must not pass judgement on another (#28)."""
    baselines = {
        "version": 2,
        "baselines": {
            "list_outbox": {
                "dataset": {"message_count": 150, "recipients": 3},
                "latency_ms": {"p95_max": 5000, "p99_max": 6000, "mean_max": 4500},
            }
        },
    }
    result = {"dataset": {"message_count": 150, "recipients": 3}, "latency_ms": {"p95": 1, "p99": 1, "mean": 1}}
    assert not check_regressions(baselines, {"list_outbox": result}).has_errors

    result["dataset"] = {"message_count": 100, "recipients": 3}
    report = check_regressions(baselines, {"list_outbox": result})
    assert [v.metric for v in report.violations] == ["dataset"]
    assert "list_outbox" not in report.passed_checks


def test_every_baselined_benchmark_is_checked(tmp_path, monkeypatch):
    """Giving a benchmark a baseline turns its check on: results are loaded for every benchmark
    baselines.json names. A hard-coded list once meant new baselines were never checked (#39)."""
    monkeypatch.setattr(regression, "RESULTS_DIR", tmp_path)
    slow = {"dataset": {"existing_reservations": 50}, "latency_ms": {"p95": 900.0, "p99": 900.0, "mean": 900.0}}
    (tmp_path / "file_reservation_paths_conflict_1337_1792590782.json").write_text(json.dumps(slow))
    baselines = {
        "version": 2,
        "baselines": {
            "file_reservation_paths_conflict": {
                "dataset": {"existing_reservations": 50},
                "latency_ms": {"p95_max": 320, "p99_max": 340, "mean_max": 230},
            }
        },
    }
    report = check_regressions(baselines)
    assert report.has_errors and {v.benchmark for v in report.violations} == {"file_reservation_paths_conflict"}


def test_a_missing_result_fails_the_check():
    """#35: a baselined benchmark with no result (renamed, or its result name changed) is an error.
    As a warning, its limit stopped applying and the check still passed."""
    baselines = {"version": 2, "baselines": {"send_message": {"latency_ms": {"p95_max": 640}}}}
    report = check_regressions(baselines, {})
    assert report.has_errors
    assert [(v.benchmark, v.metric) for v in report.violations] == [("send_message", "missing_result")]


def test_query_counts_are_checked():
    """#35: the query-count limit fires once results record real counts."""
    baselines = {"version": 2, "baselines": {"fetch_inbox": {"latency_ms": {"p95_max": 410}, "query_stats": {"avg_per_op_max": 3}}}}
    result = {"latency_ms": {"p95": 1, "p99": 1, "mean": 1}, "query_stats": {"avg_per_op": 4.0, "total": 100}}
    report = check_regressions(baselines, {"fetch_inbox": result})
    assert [(v.benchmark, v.metric) for v in report.violations] == [("fetch_inbox", "query_avg_per_op")]


@pytest.mark.ci_regression
def test_regression_report_json_artifact():
    """Test that regression report JSON artifact is properly generated."""
    if _should_skip_regression():
        pytest.skip("Skipped by CI_REGRESSION_SKIP=1")

    report = check_regressions()
    json_path = write_report_json(report)

    assert json_path.exists(), f"JSON artifact not created at {json_path}"

    import json

    content = json.loads(json_path.read_text())
    assert "status" in content
    assert "violations" in content
    assert "warnings" in content
    assert "summary" in content


@pytest.mark.ci_regression
def test_regression_with_strict_mode():
    """Test regression check in strict mode (fails on warnings too).

    Only runs if CI_REGRESSION_STRICT=1 is set.
    """
    if not _is_strict_mode():
        pytest.skip("Strict mode not enabled (set CI_REGRESSION_STRICT=1)")

    if _should_skip_regression():
        pytest.skip("Skipped by CI_REGRESSION_SKIP=1")

    report = check_regressions()
    render_report(report)
    write_report_json(report)

    if report.has_warnings or report.has_errors:
        raise AssertionError(
            f"Strict mode: {len(report.violations)} error(s), {len(report.warnings)} warning(s) found"
        )


# Query count assertion tests
class TestQueryCountAssertions:
    """Test query count assertion utilities."""

    def test_query_count_assertion_passes_under_threshold(self):
        """Query count assertion should pass when under threshold."""
        # Simulate a passing check
        assert_query_count_lte("test_op", max_queries=5, actual=3)

    def test_query_count_assertion_fails_over_threshold(self):
        """Query count assertion should fail when over threshold."""
        with pytest.raises(AssertionError) as exc_info:
            assert_query_count_lte("test_op", max_queries=2, actual=5)

        assert "test_op" in str(exc_info.value)
        assert "expected <= 2" in str(exc_info.value)
        assert "got 5" in str(exc_info.value)


def _query_limit(benchmark: str) -> int:
    """The benchmark's query limit in baselines.json, the one source for it."""
    return math.ceil(load_baselines()["baselines"][benchmark]["query_stats"]["avg_per_op_max"])


def _assert_queries_counted(operation: str, tracker_total: int) -> None:
    """A count of 0 means the tracker didn't see the server's queries (#35), not a fast operation."""
    assert tracker_total > 0, f"{operation}: no queries counted, so the tracker isn't seeing the tool's queries"


# Integration tests that run actual benchmarks with query counting
class TestQueryCountIntegration:
    """Integration tests for query counting during actual operations.

    These tests run actual MCP tool calls and verify query counts
    stay within expected bounds.
    """

    @pytest.mark.asyncio
    @pytest.mark.ci_regression
    async def test_send_message_query_count(self, isolated_env):
        """Verify send_message stays within query count threshold."""
        from fastmcp import Client

        from mcp_agent_mail.app import build_mcp_server
        from mcp_agent_mail.db import track_queries

        server = build_mcp_server()
        max_queries = _query_limit("send_message")

        async with Client(server) as client:
            project_key = "/ci-query-test-send"
            await client.call_tool("ensure_project", {"human_key": project_key})

            agent_result = await client.call_tool(
                "create_agent_identity",
                {
                    "project_key": project_key,
                    "program": "ci-test",
                    "model": "test",
                },
            )
            agent_name = agent_result.data.get("name")

            # Track queries during send_message
            with track_queries(all_tasks=True) as tracker:  # the tool runs in the server's task (#35)
                await client.call_tool(
                    "send_message",
                    {
                        "project_key": project_key,
                        "sender_name": agent_name,
                        "to": [agent_name],
                        "subject": "CI Query Count Test",
                        "body_md": "Testing query count for CI regression.",
                    },
                )

            _assert_queries_counted("send_message", tracker.total)
            assert_query_count_lte("send_message", max_queries, tracker.total)

    @pytest.mark.asyncio
    @pytest.mark.ci_regression
    async def test_fetch_inbox_query_count(self, isolated_env):
        """Verify fetch_inbox stays within query count threshold."""
        from fastmcp import Client

        from mcp_agent_mail.app import build_mcp_server
        from mcp_agent_mail.db import track_queries

        server = build_mcp_server()
        max_queries = _query_limit("fetch_inbox")

        async with Client(server) as client:
            project_key = "/ci-query-test-inbox"
            await client.call_tool("ensure_project", {"human_key": project_key})

            agent_result = await client.call_tool(
                "create_agent_identity",
                {
                    "project_key": project_key,
                    "program": "ci-test",
                    "model": "test",
                },
            )
            agent_name = agent_result.data.get("name")

            # Send a few messages first
            for i in range(5):
                await client.call_tool(
                    "send_message",
                    {
                        "project_key": project_key,
                        "sender_name": agent_name,
                        "to": [agent_name],
                        "subject": f"Message {i}",
                        "body_md": f"Content {i}",
                    },
                )

            # Track queries during fetch_inbox
            with track_queries(all_tasks=True) as tracker:  # the tool runs in the server's task (#35)
                await client.call_tool(
                    "fetch_inbox",
                    {
                        "project_key": project_key,
                        "agent_name": agent_name,
                        "limit": 20,
                    },
                )

            _assert_queries_counted("fetch_inbox", tracker.total)
            assert_query_count_lte("fetch_inbox", max_queries, tracker.total)

    @pytest.mark.asyncio
    @pytest.mark.ci_regression
    async def test_list_outbox_query_count(self, isolated_env):
        """Verify list_outbox stays within query count threshold."""
        from fastmcp import Client

        from mcp_agent_mail.app import build_mcp_server
        from mcp_agent_mail.db import track_queries

        server = build_mcp_server()
        max_queries = _query_limit("list_outbox")

        async with Client(server) as client:
            project_key = "/ci-query-test-outbox"
            await client.call_tool("ensure_project", {"human_key": project_key})

            agent_result = await client.call_tool(
                "create_agent_identity",
                {
                    "project_key": project_key,
                    "program": "ci-test",
                    "model": "test",
                },
            )
            agent_name = agent_result.data.get("name")

            # Send a few messages first
            for i in range(5):
                await client.call_tool(
                    "send_message",
                    {
                        "project_key": project_key,
                        "sender_name": agent_name,
                        "to": [agent_name],
                        "subject": f"Outbox test {i}",
                        "body_md": f"Content {i}",
                    },
                )

            # Track queries during mailbox (list_outbox equivalent via resource)
            with track_queries(all_tasks=True) as tracker:  # the tool runs in the server's task (#35)
                resource_uri = f"resource://outbox/{agent_name}?project={project_key}&limit=20"
                await client.read_resource(resource_uri)

            _assert_queries_counted("list_outbox", tracker.total)
            assert_query_count_lte("list_outbox", max_queries, tracker.total)
