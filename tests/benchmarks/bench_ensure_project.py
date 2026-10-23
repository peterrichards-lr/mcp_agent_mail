from __future__ import annotations

import pytest

from .utils import DEFAULT_SEED, run_benchmark


@pytest.mark.asyncio
@pytest.mark.benchmark
async def test_bench_ensure_project(bench_factory):
    """Create a new project each iteration: its database row and its folder in the archive.

    All projects share one Git repository at the storage root, created once (by the harness's
    own ensure_project), so this times the per-project part, not a `git init`. That's what
    test_archive_ensure_latency measured, by calling ensure_archive directly (#39).
    """
    seed = DEFAULT_SEED
    iterations = 20

    async with bench_factory("ensure_project", seed) as harness:

        async def operation(i: int) -> None:
            await harness.call_tool("ensure_project", {"human_key": f"/bench-new-project-{seed}-{i}"})

        await run_benchmark(
            name="ensure_project",
            tool="ensure_project",
            iterations=iterations,
            seed=seed,
            dataset={"new_project_each_iteration": True},
            operation=operation,
            warmup=2,
        )
