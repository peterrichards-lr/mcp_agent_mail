"""A real server process plus an MCP session over HTTP, for tests of the cross-machine client scripts.

The fixtures are registered in conftest.py; tests import the constants and `Mail` from here.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

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


@pytest.fixture
def server_url(isolated_env, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[str]:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    monkeypatch.setenv("HTTP_RBAC_DEFAULT_ROLE", "writer")
    log = (tmp_path / "server.log").open("w")
    # The port is free when picked but not reserved, so under pytest-xdist another worker's server
    # can take it first. This server then exits, and the liveness check would reach the other one,
    # so an answer only counts while this process is still running, and a lost port is retried (#27).
    ready = False
    for _attempt in range(3):
        port = _free_port()
        url = f"http://127.0.0.1:{port}"
        # cwd=tmp_path: the server must not pick up the repo's .env.
        proc = subprocess.Popen(
            [sys.executable, "-m", "mcp_agent_mail.http", "--port", str(port)],
            cwd=tmp_path,
            env=os.environ.copy(),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        for _ in range(150):
            if proc.poll() is not None:
                break
            try:
                urllib.request.urlopen(f"{url}/health/liveness", timeout=1)
            except OSError:
                time.sleep(0.1)
                continue
            ready = proc.poll() is None
            break
        if ready or proc.poll() is None:
            break
    try:
        if not ready:
            raise RuntimeError((tmp_path / "server.log").read_text())
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        log.close()


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
