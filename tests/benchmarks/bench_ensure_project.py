from __future__ import annotations

import pytest

from .utils import DEFAULT_SEED, run_benchmark


@pytest.mark.asyncio
@pytest.mark.benchmark
async def test_bench_ensure_project(bench_factory):
    """Create a new project each iteration: its database row and its Git archive.

    The archive is a `git init` plus a first commit, the slowest part of a project's first
    use (#39; was test_archive_ensure_latency, which called ensure_archive directly).
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
