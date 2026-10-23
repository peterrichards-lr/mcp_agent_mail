from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import warnings

from httpx import ASGITransport, AsyncClient

from mcp_agent_mail import config as _config
from mcp_agent_mail.app import build_mcp_server
from mcp_agent_mail.db import ensure_schema, stop_leftover_connections
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


def test_server_exits_when_its_port_is_taken(isolated_env, tmp_path):
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
                [sys.executable, "-m", "mcp_agent_mail.http", "--port", str(port)],
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


def test_stop_leftover_connections_ends_an_orphaned_worker_thread():
    """#40: a connection that nothing will close, though something still refers to it (in the
    server, the frames of background workers cancelled while connecting), keeps its non-daemon
    worker thread waiting forever. Shutdown stops that thread."""
    import asyncio
    import threading

    import aiosqlite

    left_behind: list[aiosqlite.Connection] = []

    async def open_and_abandon() -> threading.Thread:
        connection = await aiosqlite.connect(":memory:")
        left_behind.append(connection)  # still referenced, never closed
        return connection._thread

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)
        thread = asyncio.run(open_and_abandon())
    assert thread.is_alive()
    assert stop_leftover_connections() >= 1
    thread.join(timeout=5)
    assert not thread.is_alive()


