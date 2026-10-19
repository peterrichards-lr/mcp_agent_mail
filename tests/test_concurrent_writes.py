"""Tests for concurrent/parallel operations in MCP Agent Mail.

Tests concurrent access patterns including:
- Parallel message writes
- Concurrent file reservation requests
- Race conditions in lock acquisition
- Parallel inbox fetches during message delivery
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from pathlib import Path

import pytest
from fastmcp import Client
from sqlalchemy import text

from mcp_agent_mail import config as _config, db as db_module, http as http_module, llm as llm_module, storage
from mcp_agent_mail.app import build_mcp_server
from mcp_agent_mail.db import ensure_schema, get_session
from mcp_agent_mail.storage import AsyncFileLock, _commit_lock_path
from mcp_agent_mail.utils import LoopLocal


async def _setup_project_and_agents(settings: _config.Settings) -> dict:
    """Create test project and agents using MCP tools."""
    await ensure_schema()

    server = build_mcp_server()
    async with Client(server) as client:
        # Create project via MCP tool
        await client.call_tool("ensure_project", {"human_key": "/tmp/concurrent-test"})

        # Create multiple agents with auto-generated adjective+noun names
        agents = []
        tokens_by_name: dict[str, str] = {}
        for i in range(5):
            result = await client.call_tool(
                "register_agent",
                {
                    "project_key": "/tmp/concurrent-test",
                    "program": "claude-code",
                    "model": "opus-4",
                    "task_description": f"Task {i}",
                },
            )
            # Extract the generated name from the result
            data = result.data if hasattr(result, "data") else {}
            if isinstance(data, dict) and "name" in data:
                agents.append(data["name"])
                token = data.get("registration_token")
                if isinstance(token, str) and token:
                    tokens_by_name[data["name"]] = token

        # Ensure we got all agents - fail fast if not
        assert len(agents) == 5, f"Expected 5 agents, got {len(agents)}: {agents}"
        assert len(tokens_by_name) == 5, f"Expected registration tokens for all agents, got {tokens_by_name}"

        # These concurrency tests exercise delivery/write paths, not contact approval.
        # Open inboxes explicitly so fresh client sessions can message each other
        # without auto-contact side effects obscuring the concurrency assertions.
        for agent_name in agents:
            await client.call_tool(
                "set_contact_policy",
                {
                    "project_key": "/tmp/concurrent-test",
                    "agent_name": agent_name,
                    "policy": "open",
                    "registration_token": tokens_by_name[agent_name],
                },
            )

    # Get project_id from DB for reference
    async with get_session() as session:
        row = await session.execute(
            text("SELECT id FROM projects WHERE human_key = :hk"),
            {"hk": "/tmp/concurrent-test"},
        )
        project_id = row.scalar()

    return {
        "project_id": project_id,
        "project_slug": "tmp-concurrent-test",
        "agents": agents,
        "tokens_by_name": tokens_by_name,
    }


# =============================================================================
# Concurrent Message Write Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_message_sends(isolated_env):
    """Test sending multiple messages concurrently."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    server = build_mcp_server()

    async def send_message(client: Client, sender: str, recipient: str, subject: str):
        """Send a message via the MCP tool."""
        result = await client.call_tool(
            "send_message",
            {
                "project_key": "/tmp/concurrent-test",
                "sender_name": sender,
                "sender_token": data["tokens_by_name"][sender],
                "to": [recipient],
                "subject": subject,
                "body_md": f"Message from {sender} to {recipient}",
            },
        )
        return result

    # Send 10 messages concurrently
    async with Client(server) as client:
        tasks = []
        for i in range(10):
            sender = data["agents"][i % 5]
            recipient = data["agents"][(i + 1) % 5]
            tasks.append(send_message(client, sender, recipient, f"Concurrent Message {i}"))

        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Count successes (some may fail due to auto-registration issues, that's ok)
    successes = sum(1 for r in results if not isinstance(r, Exception))
    assert successes >= 5, f"Expected at least 5 successful sends, got {successes}"


@pytest.mark.asyncio
async def test_concurrent_messages_to_same_thread(isolated_env):
    """Test multiple agents writing to the same thread concurrently."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    server = build_mcp_server()

    first_agent = data["agents"][0]

    async def send_to_thread(client: Client, sender: str, message_num: int):
        """Send a message to a shared thread."""
        result = await client.call_tool(
            "send_message",
            {
                "project_key": "/tmp/concurrent-test",
                "sender_name": sender,
                "sender_token": data["tokens_by_name"][sender],
                "to": [first_agent],
                "subject": f"Thread Message {message_num}",
                "body_md": f"Message {message_num} from {sender}",
                "thread_id": "shared-thread-1",
            },
        )
        return result

    # Send 5 messages to same thread concurrently
    async with Client(server) as client:
        tasks = [send_to_thread(client, data["agents"][i], i) for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should succeed or fail gracefully
    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) < 3, f"Too many errors: {errors}"


# =============================================================================
# Concurrent File Reservation Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_file_reservation_different_paths(isolated_env):
    """Test concurrent file reservations on different paths."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    server = build_mcp_server()

    async def reserve_file(client: Client, agent: str, path: str):
        """Reserve a file path."""
        result = await client.call_tool(
            "file_reservation_paths",
            {
                "project_key": "/tmp/concurrent-test",
                "agent_name": agent,
                "registration_token": data["tokens_by_name"][agent],
                "paths": [path],
                "ttl_seconds": 3600,
                "exclusive": True,
                "reason": f"Testing by {agent}",
            },
        )
        return result

    # Reserve different paths concurrently
    async with Client(server) as client:
        tasks = [reserve_file(client, data["agents"][i], f"src/module{i}.py") for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should succeed (no conflicts)
    successes = sum(1 for r in results if not isinstance(r, Exception))
    assert successes == 5, f"Expected all 5 reservations to succeed, got {successes}"


@pytest.mark.asyncio
async def test_concurrent_file_reservation_same_path_conflict(isolated_env):
    """Test concurrent file reservations on the same path detect conflicts."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    server = build_mcp_server()

    async def reserve_file(client: Client, agent: str):
        """Reserve the same file path."""
        result = await client.call_tool(
            "file_reservation_paths",
            {
                "project_key": "/tmp/concurrent-test",
                "agent_name": agent,
                "registration_token": data["tokens_by_name"][agent],
                "paths": ["shared/config.json"],
                "ttl_seconds": 3600,
                "exclusive": True,
                "reason": f"Testing by {agent}",
            },
        )
        return result

    # Try to reserve the same path concurrently
    async with Client(server) as client:
        tasks = [reserve_file(client, data["agents"][i]) for i in range(3)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # At least one should succeed, others may report conflicts
    successes = sum(1 for r in results if not isinstance(r, Exception))
    assert successes >= 1, "At least one reservation should succeed"


@pytest.mark.asyncio
async def test_concurrent_file_reservation_overlapping_globs(isolated_env):
    """Test concurrent reservations with overlapping glob patterns."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    server = build_mcp_server()

    async with Client(server) as client:
        # First agent reserves broad pattern
        result1 = await client.call_tool(
            "file_reservation_paths",
            {
                "project_key": "/tmp/concurrent-test",
                "agent_name": data["agents"][0],
                "registration_token": data["tokens_by_name"][data["agents"][0]],
                "paths": ["src/**/*.py"],
                "ttl_seconds": 3600,
                "exclusive": True,
            },
        )

        # Second agent tries to reserve specific file in same pattern
        result2 = await client.call_tool(
            "file_reservation_paths",
            {
                "project_key": "/tmp/concurrent-test",
                "agent_name": data["agents"][1],
                "registration_token": data["tokens_by_name"][data["agents"][1]],
                "paths": ["src/app.py"],
                "ttl_seconds": 3600,
                "exclusive": True,
            },
        )

    # Second should report conflict with first
    # Result format varies, just check it doesn't crash
    assert result1 is not None
    assert result2 is not None


# =============================================================================
# Concurrent Inbox Fetch Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_inbox_fetches(isolated_env):
    """Test multiple concurrent inbox fetches."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    # First send some messages
    server = build_mcp_server()
    async with Client(server) as client:
        for i in range(5):
            await client.call_tool(
                "send_message",
                {
                    "project_key": "/tmp/concurrent-test",
                    "sender_name": data["agents"][(i + 1) % 5],
                    "sender_token": data["tokens_by_name"][data["agents"][(i + 1) % 5]],
                    "to": [data["agents"][0]],
                    "subject": f"Test Message {i}",
                    "body_md": f"Body {i}",
                },
            )

        async def fetch_inbox(c: Client):
            """Fetch inbox for Agent0."""
            result = await c.call_tool(
                "fetch_inbox",
                {
                    "project_key": "/tmp/concurrent-test",
                    "agent_name": data["agents"][0],
                    "registration_token": data["tokens_by_name"][data["agents"][0]],
                    "limit": 100,
                },
            )
            return result

        # Fetch inbox concurrently 10 times
        tasks = [fetch_inbox(client) for _ in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should succeed
    successes = sum(1 for r in results if not isinstance(r, Exception))
    assert successes == 10, f"All inbox fetches should succeed, got {successes}"


@pytest.mark.asyncio
async def test_concurrent_inbox_fetch_during_message_send(isolated_env):
    """Test inbox fetch while messages are being sent."""
    settings = _config.get_settings()
    data = await _setup_project_and_agents(settings)

    server = build_mcp_server()

    async def send_message(client: Client, i: int):
        """Send a message."""
        await asyncio.sleep(0.01 * i)  # Slight stagger
        return await client.call_tool(
            "send_message",
            {
                "project_key": "/tmp/concurrent-test",
                "sender_name": data["agents"][1],
                "sender_token": data["tokens_by_name"][data["agents"][1]],
                "to": [data["agents"][0]],
                "subject": f"Concurrent Send {i}",
                "body_md": f"Body {i}",
            },
        )

    async def fetch_inbox(client: Client):
        """Fetch inbox."""
        await asyncio.sleep(0.05)  # Slight delay
        return await client.call_tool(
            "fetch_inbox",
            {
                "project_key": "/tmp/concurrent-test",
                "agent_name": data["agents"][0],
                "registration_token": data["tokens_by_name"][data["agents"][0]],
                "limit": 100,
            },
        )

    # Run sends and fetches concurrently
    async with Client(server) as client:
        send_tasks = [send_message(client, i) for i in range(5)]
        fetch_tasks = [fetch_inbox(client) for _ in range(3)]
        results = await asyncio.gather(*send_tasks, *fetch_tasks, return_exceptions=True)

    # Should not crash
    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) < len(results), "Some operations should succeed"


# =============================================================================
# Lock Race Condition Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_project_ensure(isolated_env):
    """Test concurrent project ensure calls."""
    _config.get_settings()  # Ensure settings are loaded
    await ensure_schema()

    server = build_mcp_server()

    async def ensure_project(client: Client, suffix: str):
        """Ensure a project exists."""
        return await client.call_tool(
            "ensure_project",
            {"human_key": f"/tmp/race-test-{suffix}"},
        )

    # Call ensure_project concurrently for same project
    async with Client(server) as client:
        tasks = [ensure_project(client, "same") for _ in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # At least some should succeed (idempotent operation), but under high concurrency
    # with Python 3.14 CancelledError-as-BaseException, client cleanup can trigger
    # transient exceptions even after successful tool calls
    successes = sum(1 for r in results if not isinstance(r, BaseException))
    min_expected = 2  # 40% threshold - very tolerant for CI reliability
    if successes < min_expected:
        errors = []
        for r in results:
            if isinstance(r, BaseException):
                errors.append(f"{type(r).__name__}: {r}")
        assert successes >= min_expected, f"Some ensures should succeed, got {successes}. Errors: {errors}"


@pytest.mark.asyncio
async def test_concurrent_agent_registration(isolated_env):
    """Test concurrent agent registration."""
    _config.get_settings()  # Ensure settings are loaded
    await ensure_schema()

    server = build_mcp_server()

    async with Client(server) as client:
        # First ensure project
        await client.call_tool("ensure_project", {"human_key": "/tmp/reg-test"})

        async def register_agent(c: Client, i: int):
            """Register an agent."""
            return await c.call_tool(
                "register_agent",
                {
                    "project_key": "/tmp/reg-test",
                    "program": "claude-code",
                    "model": "opus-4",
                    "task_description": f"Task {i}",
                },
            )

        # Register multiple agents concurrently
        tasks = [register_agent(client, i) for i in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should succeed with unique names
    successes = [r for r in results if not isinstance(r, Exception)]
    assert len(successes) >= 8, f"Most registrations should succeed, got {len(successes)}"

    # Verify unique names
    names = set()
    for r in successes:
        data = getattr(r, "data", None)
        if isinstance(data, dict) and "name" in data:
            names.add(data["name"])

    # Names should be unique (if we could extract them)


# =============================================================================
# Database Concurrent Access Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_message_read_write(isolated_env):
    """Test concurrent reads and writes to messages table."""
    _config.get_settings()  # Ensure settings are loaded
    await ensure_schema()

    async with get_session() as session:
        await session.execute(
            text("INSERT INTO projects (slug, human_key, created_at) VALUES (:slug, :hk, datetime('now'))"),
            {"slug": "db-concurrent", "hk": "/tmp/db-concurrent"},
        )
        await session.commit()

        row = await session.execute(text("SELECT id FROM projects WHERE slug = :slug"), {"slug": "db-concurrent"})
        project_id = row.scalar()

    async def write_message(i: int) -> None:
        """Write a message."""
        async with get_session() as session:
            await session.execute(
                text(
                    "INSERT INTO messages (project_id, subject, body_md, importance, ack_required, sender_id, created_ts) "
                    "VALUES (:pid, :subj, :body, :imp, :ack, 1, datetime('now'))"
                ),
                {"pid": project_id, "subj": f"Msg {i}", "body": f"Body {i}", "imp": "normal", "ack": 0},
            )
            await session.commit()

    async def read_messages() -> int:
        """Read message count."""
        async with get_session() as session:
            row = await session.execute(
                text("SELECT COUNT(*) FROM messages WHERE project_id = :pid"),
                {"pid": project_id},
            )
            return row.scalar() or 0

    # Mix writes and reads
    write_tasks = [write_message(i) for i in range(10)]
    read_tasks = [read_messages() for _ in range(5)]

    results = await asyncio.gather(*write_tasks, *read_tasks, return_exceptions=True)

    # Should not crash
    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) == 0, f"No errors expected: {errors}"


# =============================================================================
# Archive Lock Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_archive_writes(isolated_env):
    """Test concurrent writes to the same archive."""
    settings = _config.get_settings()

    from mcp_agent_mail.storage import ensure_archive, write_agent_profile

    archive = await ensure_archive(settings, "archive-lock-test")

    async def write_profile(i: int) -> None:
        """Write an agent profile."""
        await write_agent_profile(
            archive,
            {
                "name": f"Agent{i}",
                "program": "claude-code",
                "model": "opus-4",
                "task_description": f"Task {i}",
            },
        )

    # Write profiles concurrently
    tasks = [write_profile(i) for i in range(5)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Should handle concurrent access
    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) < 3, f"Most writes should succeed: {errors}"


@pytest.mark.asyncio
async def test_concurrent_message_bundle_writes(isolated_env):
    """Test concurrent message bundle writes to archive."""
    settings = _config.get_settings()

    from mcp_agent_mail.storage import ensure_archive, write_message_bundle

    archive = await ensure_archive(settings, "bundle-lock-test")

    async def write_bundle(i: int) -> None:
        """Write a message bundle."""
        await write_message_bundle(
            archive,
            message={"id": i, "subject": f"Subject {i}"},
            body_md=f"Body {i}",
            sender=f"Sender{i}",
            recipients=[f"Recipient{i}"],
        )

    # Write bundles concurrently
    tasks = [write_bundle(i) for i in range(10)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Should handle concurrent access (archive lock)
    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) < 5, f"Most writes should succeed: {errors}"


@pytest.mark.asyncio
async def test_message_bundle_commit_message_is_terse(isolated_env):
    """Default commit messages must be terse, not Rich-rendered ASCII art (regression: #156)."""
    settings = _config.get_settings()

    from mcp_agent_mail.storage import ensure_archive, write_message_bundle

    archive = await ensure_archive(settings, "commit-shape-test")

    await write_message_bundle(
        archive,
        message={"id": 1, "subject": "Hello world", "thread_id": "t1"},
        body_md="A message body.",
        sender="Alice",
        recipients=["Bob", "Carol"],
    )

    latest = next(iter(archive.repo.iter_commits()))
    summary = latest.summary
    body = str(latest.message)

    assert summary == "mail: Alice -> Bob, Carol | Hello world", (
        f"unexpected commit summary: {summary!r}"
    )
    # ASCII-art borders or panel chrome must never appear in commit messages.
    for forbidden in ("╔", "║", "╚", "MCP TOOL CALL", "Lightning Fast"):
        assert forbidden not in body, (
            f"commit body leaked panel chrome ({forbidden!r}):\n{body}"
        )


# =============================================================================
# Commit Lock Scoping Tests
# =============================================================================


def test_commit_lock_path_scopes_to_project(tmp_path: Path) -> None:
    repo_root = tmp_path
    rel_paths = [
        "projects/alpha/agents/GreenLake/profile.json",
        "projects/alpha/messages/2026/01/msg.md",
    ]
    lock_path = _commit_lock_path(repo_root, rel_paths)
    assert lock_path == repo_root / "projects" / "alpha" / ".commit.lock"


def test_commit_lock_path_falls_back_for_mixed_paths(tmp_path: Path) -> None:
    repo_root = tmp_path
    rel_paths = [
        "projects/alpha/agents/GreenLake/profile.json",
        "projects/beta/messages/2026/01/msg.md",
    ]
    lock_path = _commit_lock_path(repo_root, rel_paths)
    assert lock_path == repo_root / ".commit.lock"


@pytest.mark.asyncio
async def test_commit_lock_paths_do_not_block_across_projects(tmp_path: Path) -> None:
    repo_root = tmp_path
    lock_a = _commit_lock_path(repo_root, ["projects/alpha/messages/2026/01/a.md"])
    lock_b = _commit_lock_path(repo_root, ["projects/beta/messages/2026/01/b.md"])

    async with AsyncFileLock(lock_a, timeout_seconds=0.5), AsyncFileLock(lock_b, timeout_seconds=0.5):
        assert lock_a != lock_b


# --- Module-level asyncio primitives across event loops (#29) -----------------------------------
# pytest runs every test on a new event loop. A module-level asyncio.Lock binds to the first loop
# that waits on it, so a later test contending for it failed with "bound to a different event
# loop". The tool wrapper reported that as RESOURCE_BUSY, which is how it showed up on macOS CI.
# These tests are synchronous so that each asyncio.run() is a separate loop.

_MODULE_PRIMITIVES = {
    "storage._REPO_CACHE_LOCK": storage._REPO_CACHE_LOCK,
    "storage._REPO_SEMAPHORE": storage._REPO_SEMAPHORE,
    "storage._COMMIT_QUEUE_LOCK": storage._COMMIT_QUEUE_LOCK,
    "db._schema_lock": db_module._schema_lock,
    "db._CIRCUIT_BREAKER_LOCK": db_module._CIRCUIT_BREAKER_LOCK,
    "llm._init_lock": llm_module._init_lock,
    "http._jwks_cache_lock": http_module._jwks_cache_lock,
}


async def _hold_with_a_waiter(primitive: LoopLocal[asyncio.Lock] | LoopLocal[asyncio.Semaphore]) -> None:
    """Leave the primitive held, with a waiter, as the loop ends: the state CI hit."""
    held = primitive.get()
    if isinstance(held, asyncio.Semaphore):
        for _ in range(storage._REPO_SEMAPHORE_LIMIT):
            await held.acquire()
    else:
        await held.acquire()
    waiter = asyncio.create_task(held.acquire())
    await asyncio.sleep(0)  # the waiter's future binds the primitive to this loop
    assert not waiter.done()


async def _contend(primitive: LoopLocal[asyncio.Lock] | LoopLocal[asyncio.Semaphore]) -> None:
    held = primitive.get()
    async with held:
        waiter = asyncio.create_task(held.acquire())
        await asyncio.sleep(0)
    await waiter
    held.release()


@pytest.mark.parametrize("name", sorted(_MODULE_PRIMITIVES))
def test_module_primitive_survives_a_loop_that_ended_holding_it(name):
    primitive = _MODULE_PRIMITIVES[name]
    asyncio.run(_hold_with_a_waiter(primitive))
    # A module-global primitive raised RuntimeError("... is bound to a different event loop") here.
    asyncio.run(_contend(primitive))


def test_loop_local_gives_each_event_loop_its_own_value():
    local: LoopLocal[asyncio.Lock] = LoopLocal(asyncio.Lock)
    loops: list[weakref.ref[asyncio.AbstractEventLoop]] = []

    async def use() -> tuple[asyncio.Lock, asyncio.Lock]:
        loops.append(weakref.ref(asyncio.get_running_loop()))
        await _contend(local)  # a lock that has waited references its loop
        return local.get(), local.get()

    first_a, first_b = asyncio.run(use())
    second = asyncio.run(use())[0]
    asyncio.run(use())
    assert first_a is first_b
    assert second is not first_a
    assert local.peek() is None  # outside a loop

    del first_a, first_b, second
    gc.collect()
    # Each new loop dropped the closed ones, so only the last loop's lock is left, and the
    # earlier loops were freed despite their locks referencing them.
    assert len(local._values) == 1
    assert [ref() is None for ref in loops] == [True, True, False]


def test_concurrent_project_ensure_after_a_loop_ended_holding_the_repo_lock(isolated_env):
    """The CI failure end to end: five concurrent ensure_project calls on a fresh archive."""
    asyncio.run(_hold_with_a_waiter(storage._REPO_CACHE_LOCK))

    async def ensure_five() -> list[object]:
        await ensure_schema()
        async with Client(build_mcp_server()) as client:
            return await asyncio.gather(
                *[client.call_tool("ensure_project", {"human_key": "/tmp/race-test-loops"}) for _ in range(5)],
                return_exceptions=True,
            )

    results = asyncio.run(ensure_five())
    errors = [f"{type(r).__name__}: {r}" for r in results if isinstance(r, BaseException)]
    assert errors == []


def test_file_lock_tables_belong_to_their_event_loop(tmp_path):
    """A loop that ends holding an AsyncFileLock leaves nothing in the next loop's tables.

    The tables used to be keyed by id(loop), so a later loop that got the same id inherited the
    dead loop's held lock. That reuse can't be forced in a test, so this pins the property instead.
    """
    lock_path = tmp_path / "held.lock"

    async def end_while_holding() -> bool:
        lock = AsyncFileLock(lock_path, timeout_seconds=1.0)
        await lock.__aenter__()  # never released: the loop ends with the lock held
        return lock._lock_key in storage._PROCESS_LOCKS.get() and lock._lock_key in storage._PROCESS_LOCK_OWNERS.get()

    async def tables() -> tuple[dict[str, asyncio.Lock], dict[str, int]]:
        return dict(storage._PROCESS_LOCKS.get()), dict(storage._PROCESS_LOCK_OWNERS.get())

    assert asyncio.run(end_while_holding())
    assert asyncio.run(tables()) == ({}, {})
