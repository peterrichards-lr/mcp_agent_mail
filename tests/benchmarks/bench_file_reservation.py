from __future__ import annotations

import pytest

from .utils import DEFAULT_SEED, run_benchmark


@pytest.mark.asyncio
@pytest.mark.benchmark
async def test_bench_file_reservation_conflict(bench_factory):
    """Reserve a path that overlaps one of 50 exclusive reservations another agent holds.

    Every request is checked against all active reservations in the project, so this is the
    conflict check's cost as reservations pile up (#39; was test_reservation_conflict_check_with_100).
    """
    seed = DEFAULT_SEED
    iterations = 25
    reservation_count = 50

    async with bench_factory("file_reservation_conflict", seed) as harness:
        for i in range(reservation_count):
            await harness.call_tool(
                "file_reservation_paths",
                {
                    "project_key": harness.project_key,
                    "agent_name": harness.agent_name,
                    "paths": [f"lib/component_{i}/**"],
                    "ttl_seconds": 3600,
                    "exclusive": True,
                },
            )
        other = await harness.call_tool(
            "create_agent_identity",
            {"project_key": harness.project_key, "program": "benchmark", "model": "test"},
        )

        async def operation(i: int) -> None:
            await harness.call_tool(
                "file_reservation_paths",
                {
                    "project_key": harness.project_key,
                    "agent_name": other["name"],
                    "paths": [f"lib/component_{i % reservation_count}/**"],
                    "ttl_seconds": 3600,
                    "exclusive": True,
                },
            )

        await run_benchmark(
            name="file_reservation_conflict",
            tool="file_reservation_paths",
            iterations=iterations,
            seed=seed,
            dataset={"existing_reservations": reservation_count, "exclusive": True},
            operation=operation,
            warmup=2,
        )
