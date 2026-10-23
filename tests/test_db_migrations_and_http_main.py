from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys

import pytest
from httpx import ASGITransport, AsyncClient

from mcp_agent_mail import config as _config
from mcp_agent_mail.app import build_mcp_server
from mcp_agent_mail.db import ensure_schema
from mcp_agent_mail.http import build_http_app, main as http_main


def test_http_main_invokes_uvicorn(monkeypatch):
    # Ensure settings default host/port are used
    with contextlib.suppress(Exception):
        _config.clear_settings_cache()
    calls: dict[str, object] = {}

    def fake_run(app, host, port, log_level="info"):
        calls["host"] = host
        calls["port"] = port
        calls["lv"] = log_level

    monkeypatch.setenv("HTTP_HOST", "127.0.0.1")
    monkeypatch.setenv("HTTP_PORT", "8765")
    monkeypatch.setattr("uvicorn.run", fake_run)
    # Prevent pytest argv from leaking into argparse
    monkeypatch.setattr(sys, "argv", ["mcp-http"])
    http_main()
    assert calls.get("host") == "127.0.0.1"


async def _readiness_ok() -> int:
    # Sanity check app readiness OK path with schema ensured
    await ensure_schema()
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/health/readiness")
        return r.status_code


def test_readiness_ok_status(isolated_env):
    import asyncio

    code = asyncio.run(_readiness_ok())
    assert code in (200, 503)


@pytest.mark.parametrize(
    "entry_point", [["mcp_agent_mail.http"], ["mcp_agent_mail.cli", "serve-http"]], ids=["http-main", "cli-serve-http"]
)
def test_server_exits_when_its_port_is_taken(isolated_env, tmp_path, entry_point):
    """#40, against a real server process: when another process holds the port, the server logs
    the bind error and exits with 1. It used to shut down and then never exit: background workers
    cancelled at startup left aiosqlite worker threads running, which aren't daemons."""
    with socket.socket() as holder:
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        port = holder.getsockname()[1]
        log = tmp_path / "server.log"
        with log.open("w") as out:
            # cwd=tmp_path: the server must not pick up the repo's .env.
            proc = subprocess.Popen(
                [sys.executable, "-m", *entry_point, "--port", str(port)],
                cwd=tmp_path, env=os.environ.copy(), stdout=out, stderr=subprocess.STDOUT,
            )
            try:
                code = proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
                raise AssertionError(f"the server never exited after losing its port:\n{log.read_text()}") from None
    text = log.read_text(errors="replace")
    assert code == 1, text
    assert "error while attempting to bind on address" in text


_ORPHANED_CONNECTION = """
import asyncio, sys, warnings
import aiosqlite
from mcp_agent_mail.db import stop_leftover_connections

warnings.simplefilter("ignore", ResourceWarning)  # the connection is left open on purpose
left_behind = []

async def open_and_abandon():
    connection = await aiosqlite.connect(":memory:")
    left_behind.append(connection)  # still referenced, never closed
    return connection._thread

thread = asyncio.run(open_and_abandon())
assert thread.is_alive(), "the worker thread should outlive the event loop"
stopped = stop_leftover_connections()
thread.join(timeout=5)
print(f"stopped={stopped} alive={thread.is_alive()}")
"""


def test_stop_leftover_connections_ends_an_orphaned_worker_thread(tmp_path):
    """#40: a connection that nothing will close, though something still refers to it (in the
    server, the frames of background workers cancelled while connecting), keeps its non-daemon
    worker thread waiting forever, and the process with it. stop_leftover_connections ends it.

    In a separate process: the function stops every aiosqlite thread in its process, which in
    this test process would include other tests' connections."""
    done = subprocess.run(
        [sys.executable, "-c", _ORPHANED_CONNECTION],
        cwd=tmp_path, env=os.environ.copy(), capture_output=True, text=True, timeout=60,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "stopped=1 alive=False", done.stdout + done.stderr


