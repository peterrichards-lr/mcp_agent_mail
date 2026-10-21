"""A real server process plus an MCP session over HTTP, for tests of the cross-machine client scripts.

The fixtures are registered in conftest.py; tests import the constants and `Mail` from here.
"""

from __future__ import annotations

import contextlib
import http.client
import os
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import psutil
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

TOKEN = "wake-test-token"
PROJECT = "github.com/owner/wake"


def client_env(**extra: str) -> dict[str, str]:
    """A minimal environment for client-script subprocesses: PATH, plus what Windows needs to
    run Python at all (SYSTEMROOT for sockets, USERPROFILE for the home folder)."""
    env = {"PATH": os.environ["PATH"]}
    for name in ("SYSTEMROOT", "COMSPEC", "PATHEXT", "TEMP", "TMP", "OS", "USERPROFILE", "LOCALAPPDATA", "APPDATA"):
        if name in os.environ:
            env[name] = os.environ[name]
    return env | extra


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _listens_on(proc: subprocess.Popen[bytes], port: int) -> bool:
    """Whether `proc`, or a process it started, holds a listening socket on `port`.

    On Windows a venv's python.exe is a launcher that runs the real interpreter as its child, so
    the socket belongs to the child.
    """
    try:
        root = psutil.Process(proc.pid)
        processes = [root, *root.children(recursive=True)]
    except psutil.Error:
        return False
    for process in processes:
        try:
            connections = process.net_connections(kind="tcp")
        except psutil.Error:
            continue
        if any(c.status == psutil.CONN_LISTEN and c.laddr and c.laddr.port == port for c in connections):
            return True
    return False


# uvicorn logs this when the bind fails, on every OS; the reason after it differs (Windows says
# "only one usage of each socket address", not "address already in use").
_BIND_FAILED = "error while attempting to bind on address"


def _wait_for_server(proc: subprocess.Popen[bytes], port: int, url: str, log_path: Path) -> str:
    """"serving" once `proc` itself listens on `port` and answers, "port taken" if another process
    bound it first, or "failed" (it exited, or nothing within 15 seconds).

    The port is free when picked but not reserved, so under pytest-xdist another worker's server
    can take it first. That server would answer the liveness check while this one is still
    importing, so an answer only counts once this process holds the listening socket (#27). A
    server that loses the port logs it but doesn't exit (#40), so the log is what says so.
    """
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if _BIND_FAILED in log_path.read_text(errors="replace"):
            return "port taken"
        if proc.poll() is not None:
            return "failed"
        if _listens_on(proc, port):
            try:
                with urllib.request.urlopen(f"{url}/health/liveness", timeout=1):
                    return "serving"
            except (OSError, http.client.HTTPException):
                pass
        time.sleep(0.1)
    return "failed"


def _stop(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


@pytest.fixture
def server_url(isolated_env, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[str]:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    monkeypatch.setenv("HTTP_RBAC_DEFAULT_ROLE", "writer")
    # Every attempt's process is stopped and its log closed on the way out, even if one stop fails.
    with contextlib.ExitStack() as cleanup:
        # A port lost to another worker's server is retried on a new one.
        for attempt in range(3):
            port = _free_port()
            url = f"http://127.0.0.1:{port}"
            log_path = tmp_path / f"server-{attempt}.log"
            log = cleanup.enter_context(log_path.open("w"))
            # cwd=tmp_path: the server must not pick up the repo's .env.
            proc = subprocess.Popen(
                [sys.executable, "-m", "mcp_agent_mail.http", "--port", str(port)],
                cwd=tmp_path,
                env=os.environ.copy(),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            cleanup.callback(_stop, proc)
            state = _wait_for_server(proc, port, url, log_path)
            if state == "serving":
                break
            if state == "failed":
                raise RuntimeError(log_path.read_text(errors="replace"))
            _stop(proc)
        else:
            raise RuntimeError("the server lost its port to another process three times")
        yield url


class Mail:
    """An MCP session over real HTTP that can send as any of the registered agents."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.tokens: dict[str, str] = {}

    async def send(self, sender: str, to: list[str], subject: str, **extra: Any) -> int:
        result = await self.client.call_tool(
            "send_message",
            {
                "project_key": PROJECT,
                "sender_name": sender,
                "sender_token": self.tokens[sender],
                "to": to,
                "subject": subject,
                "body_md": "SECRET BODY",
                **extra,
            },
        )
        return int(result.data["deliveries"][0]["payload"]["id"])


@pytest.fixture
async def mcp(server_url: str) -> AsyncIterator[Mail]:
    transport = StreamableHttpTransport(f"{server_url}/mcp/", headers={"Authorization": f"Bearer {TOKEN}"})
    async with Client(transport) as client:
        mail = Mail(client)
        await client.call_tool("ensure_project", {"human_key": PROJECT})
        for name in ("BlueLake", "GreenCastle", "RedStone"):
            result = await client.call_tool(
                "register_agent", {"project_key": PROJECT, "program": "claude-code", "model": "opus", "name": name}
            )
            mail.tokens[name] = result.data["registration_token"]
            # Contact approval is not what these tests are about.
            await client.call_tool(
                "set_contact_policy",
                {"project_key": PROJECT, "agent_name": name, "policy": "open", "registration_token": mail.tokens[name]},
            )
        yield mail
