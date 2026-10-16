import asyncio
import contextlib
import gc
import warnings
import weakref
from pathlib import Path
from typing import Any

import psutil
import pytest
from git import Repo

from mcp_agent_mail.config import clear_settings_cache
from mcp_agent_mail.db import reset_database_state
from mcp_agent_mail.storage import clear_repo_cache
from tests.live_server import mcp, server_url  # noqa: F401  (real-server fixtures for the client-script tests)

# Every GitPython Repo created during the run, so teardown can close the ones a test left open
# (each holds file descriptors and `git cat-file` subprocesses; under macOS's low ulimit, leaks
# cascade into EMFILE later in the suite). This replaces walking the whole heap with
# gc.get_objects() after every test, which cost seconds per test on CI runners (#27).
_LIVE_REPOS: "weakref.WeakSet[Repo]" = weakref.WeakSet()
_original_repo_init = Repo.__init__


def _tracking_repo_init(self: Repo, *args: Any, **kwargs: Any) -> None:
    _original_repo_init(self, *args, **kwargs)
    _LIVE_REPOS.add(self)


Repo.__init__ = _tracking_repo_init


def _close_live_repos() -> None:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=ResourceWarning)
        for repo in list(_LIVE_REPOS):
            with contextlib.suppress(Exception):
                repo.close()

# CPU overload threshold - skip benchmark tests if ALL cores are at this level
CPU_OVERLOAD_THRESHOLD = 95.0


def is_cpu_overloaded() -> bool:
    """Check if all CPU cores are at 95%+ utilization.

    Returns True only when the system is under extreme load (all cores saturated),
    which would make timing-based benchmark tests unreliable.
    """
    # Sample CPU usage over 200ms per-core
    per_cpu = psutil.cpu_percent(interval=0.2, percpu=True)
    if not per_cpu:
        return False

    overloaded = sum(1 for usage in per_cpu if usage >= CPU_OVERLOAD_THRESHOLD)
    return overloaded == len(per_cpu)


def skip_if_cpu_overloaded() -> None:
    """Skip the current test if all CPU cores are at 95%+ utilization.

    Use this at the start of any test that asserts on wall-clock time.
    Prevents flaky benchmark tests when the system is under extreme load.
    """
    if is_cpu_overloaded():
        cores = psutil.cpu_count()
        pytest.skip(
            f"Skipping benchmark: system under extreme CPU load "
            f"(all {cores} cores at {CPU_OVERLOAD_THRESHOLD}%+ utilization)"
        )


@pytest.fixture(scope="function")
def event_loop():
    """Create a new event loop for each test function.

    This fixture ensures proper event loop cleanup on all platforms,
    particularly macOS where the default event loop policy can cause
    'Event loop is closed' errors if not handled properly.

    The fixture:
    1. Creates a fresh event loop for each test
    2. Properly shuts down async generators
    3. Cancels any pending tasks
    4. Closes the loop cleanly

    Note: In Python 3.14+, event loop policy management is deprecated.
    asyncio.new_event_loop() creates the appropriate loop type automatically.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    yield loop

    # Proper cleanup sequence
    try:
        # Cancel all pending tasks
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()

        # Allow cancelled tasks to complete
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        # Shutdown async generators (Python 3.6+)
        loop.run_until_complete(loop.shutdown_asyncgens())

        # Shutdown default executor (Python 3.9+)
        if hasattr(loop, "shutdown_default_executor"):
            loop.run_until_complete(loop.shutdown_default_executor())
    except Exception:
        pass  # Ignore cleanup errors
    finally:
        asyncio.set_event_loop(None)
        loop.close()


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    """Provide isolated database settings for tests and reset caches."""
    db_path: Path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("HTTP_HOST", "127.0.0.1")
    monkeypatch.setenv("HTTP_PORT", "8765")
    monkeypatch.setenv("HTTP_PATH", "/mcp/")
    monkeypatch.setenv("APP_ENVIRONMENT", "test")
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv("GIT_AUTHOR_NAME", "test-agent")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "test@example.com")
    monkeypatch.setenv("INLINE_IMAGE_MAX_BYTES", "128")
    clear_settings_cache()
    reset_database_state()
    # Clear repo cache before test to ensure isolation
    clear_repo_cache()
    try:
        yield
    finally:
        # Close all cached Repo objects first (prevents file handle leaks)
        clear_repo_cache()
        # Dispose database engine/pool state before the collection below, so SQLAlchemy
        # returns pooled connections cleanly instead of warning during finalizers.
        reset_database_state()
        # Repos the test opened outside the cache, then one collection to finalize anything
        # else still holding the files deleted below. On Windows an open file can't be
        # deleted (WinError 32), and leftover SQLite connections are only closed by gc.
        _close_live_repos()
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=ResourceWarning)
            gc.collect()

        clear_settings_cache()

        if db_path.exists():
            db_path.unlink()
        storage_root = tmp_path / "storage"
        if storage_root.exists():
            for path in storage_root.rglob("*"):
                if path.is_file():
                    path.unlink()
            for path in sorted(storage_root.rglob("*"), reverse=True):
                if path.is_dir():
                    path.rmdir()
            if storage_root.exists():
                storage_root.rmdir()


@pytest.fixture(autouse=True)
def _global_resource_cleanup():
    """Best-effort global cleanup to avoid FD leaks under low ulimit.

    Some tests don't opt into `isolated_env` but still touch the global engine/repo cache.
    With RLIMIT_NOFILE=256 (common on macOS), a small amount of leakage can cascade into
    EMFILE failures later in the suite.
    """
    yield

    # Close cached repo handles first.
    with contextlib.suppress(Exception):
        clear_repo_cache()

    # Dispose engine/pool state across tests.
    with contextlib.suppress(Exception):
        reset_database_state()

    with contextlib.suppress(Exception):
        clear_settings_cache()

    # In-process auth caches outlive a test's database. Tests reuse the same fake JWKS URL with
    # freshly generated keys under the same kid, so a key cached by one test failed the next
    # test's tokens (a 401 only in full-suite order).
    with contextlib.suppress(Exception):
        from mcp_agent_mail import http as _http, ip_guard, oidc, ui_auth

        for cache in (
            _http._jwks_cache, oidc._discovery_cache, oidc._jwks_cache, ip_guard._ban_cache,
            ui_auth._token_cache, ui_auth._session_cache, ui_auth._account_cache,
        ):
            cache.clear()

    # Close any Repo objects that escaped caching, then one collection to finalize other
    # resources (sockets, files) held only by reference cycles.
    _close_live_repos()
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=ResourceWarning)
        gc.collect()
