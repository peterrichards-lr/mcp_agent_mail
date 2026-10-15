"""Push delivery: scripts/hooks/mail_wake.py against a real server process over HTTP."""

from __future__ import annotations

import asyncio
import json
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

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "hooks" / "mail_wake.py"
TOKEN = "wake-test-token"
PROJECT = "github.com/owner/wake"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def server_url(isolated_env, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[str]:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    monkeypatch.setenv("HTTP_RBAC_DEFAULT_ROLE", "writer")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    log = (tmp_path / "server.log").open("w")
    # cwd=tmp_path: the server must not pick up the repo's .env.
    proc = subprocess.Popen(
        [sys.executable, "-m", "mcp_agent_mail.http", "--port", str(port)],
        cwd=tmp_path,
        env=os.environ.copy(),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        for _ in range(150):
            try:
                urllib.request.urlopen(f"{url}/health/liveness", timeout=1)
                break
            except OSError:
                time.sleep(0.1)
        else:
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


def _env(server_url: str, state: Path, **overrides: str) -> dict[str, str]:
    env = {
        "PATH": os.environ["PATH"],
        "AGENT_MAIL_SERVER_URL": server_url,
        "AGENT_MAIL_TOKEN": TOKEN,
        "AGENT_MAIL_PROJECT": PROJECT,
        "AGENT_MAIL_AGENT": "GreenCastle",
        "AGENT_MAIL_WAKE_STATE_DIR": str(state),
        "AGENT_MAIL_WAKE_WAIT_SECONDS": "20",
    }
    return env | overrides


async def _start(command: str, env: dict[str, str], hook_input: dict[str, Any]) -> asyncio.subprocess.Process:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(SCRIPT), command, env=env,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdin is not None
    proc.stdin.write(json.dumps(hook_input).encode())
    await proc.stdin.drain()
    proc.stdin.close()
    return proc


async def _finish(proc: asyncio.subprocess.Process, within: float = 30) -> tuple[int, str, str]:
    stdout, stderr = await asyncio.wait_for(proc.communicate(), within)
    assert proc.returncode is not None
    return proc.returncode, stdout.decode(), stderr.decode()


async def _run(command: str, env: dict[str, str], hook_input: dict[str, Any] | None = None) -> tuple[int, str, str]:
    return await _finish(await _start(command, env, hook_input or {}))


def _agent_state(state: Path) -> Path:
    return state / "github.com_owner_wake__GreenCastle"


async def _wait_for_cursor(state: Path) -> None:
    for _ in range(100):
        if (_agent_state(state) / "cursor.json").exists():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("waiter never initialised its cursor")


STOP = {"hook_event_name": "Stop", "session_id": "s1"}


@pytest.mark.asyncio
async def test_wait_wakes_only_for_mail_addressed_to_the_agent(mcp: Mail,server_url: str, tmp_path: Path) -> None:
    await mcp.send("BlueLake", ["GreenCastle"], "old mail, before the waiter")
    env = _env(server_url, tmp_path / "state")
    waiter = await _start("wait", env, STOP)
    await _wait_for_cursor(tmp_path / "state")

    await mcp.send("GreenCastle", ["BlueLake"], "its own send")
    await mcp.send("BlueLake", ["RedStone"], "mail for someone else")
    mid = await mcp.send("BlueLake", ["GreenCastle"], "Can you review the API?", thread_id="br-7", ack_required=True)

    code, stdout, stderr = await _finish(waiter)
    assert code == 2, stderr
    assert stdout == ""
    assert f'#{mid} from BlueLake: "Can you review the API?" (ack required, thread br-7)' in stderr
    assert "1 new message for GreenCastle" in stderr
    for leaked in ("old mail", "its own send", "someone else", "SECRET BODY"):
        assert leaked not in stderr


@pytest.mark.asyncio
async def test_only_one_waiter_per_agent_and_timeout_is_silent(mcp: Mail,server_url: str, tmp_path: Path) -> None:
    env = _env(server_url, tmp_path / "state", AGENT_MAIL_WAKE_WAIT_SECONDS="3")
    first = await _start("wait", env, STOP)
    await _wait_for_cursor(tmp_path / "state")

    assert await _run("wait", env, STOP) == (0, "", "")  # a second Stop while the first waits
    assert await _finish(first) == (0, "", "")  # no mail: times out without waking


@pytest.mark.asyncio
async def test_usage_limit_holds_mail_for_the_next_prompt(mcp: Mail,server_url: str, tmp_path: Path) -> None:
    state = tmp_path / "state"
    usage = {"rate_limits": {"five_hour": {"used_percentage": 91.4}, "seven_day": {"used_percentage": 30}}}
    code, stdout, _ = await _run("record-usage", _env(server_url, state), usage)
    assert (code, stdout.strip()) == (0, "five-hour 91% · seven-day 30%")

    env = _env(server_url, state, AGENT_MAIL_WAKE_MAX_USAGE_PCT="80")
    waiter = await _start("wait", env, STOP)
    await _wait_for_cursor(state)
    await mcp.send("BlueLake", ["GreenCastle"], "held back")
    assert await _finish(waiter) == (0, "", "")
    assert "five-hour plan usage 91% >= 80%" in (_agent_state(state) / "wake.log").read_text()

    code, stdout, _ = await _run("context", env, {"hook_event_name": "UserPromptSubmit", "session_id": "s1"})
    output = json.loads(stdout)["hookSpecificOutput"]
    assert output["hookEventName"] == "UserPromptSubmit"
    assert '"held back"' in output["additionalContext"]
    assert "held back earlier by this agent's token limits" in output["additionalContext"]
    # Reported once only.
    assert await _run("context", env, {"hook_event_name": "UserPromptSubmit", "session_id": "s1"}) == (0, "", "")


@pytest.mark.asyncio
async def test_usage_is_tracked_per_account(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    """One machine, two Claude accounts (CLAUDE_CONFIG_DIR): a busy work plan must not block the personal one."""
    state = tmp_path / "state"
    busy = {"rate_limits": {"five_hour": {"used_percentage": 95}}}
    await _run("record-usage", _env(server_url, state, CLAUDE_CONFIG_DIR="/home/u/.claude-work"), busy)

    env = _env(server_url, state, CLAUDE_CONFIG_DIR="/home/u/.claude-personal", AGENT_MAIL_WAKE_MAX_USAGE_PCT="80")
    waiter = await _start("wait", env, STOP)
    await _wait_for_cursor(state)
    await mcp.send("BlueLake", ["GreenCastle"], "personal account mail")
    assert (await _finish(waiter))[0] == 2
    assert sorted(p.name for p in state.glob("usage-*.json")) == ["usage-.claude-work.json"]


@pytest.mark.asyncio
async def test_wake_cap_per_hour(mcp: Mail,server_url: str, tmp_path: Path) -> None:
    env = _env(server_url, tmp_path / "state", AGENT_MAIL_WAKE_MAX_PER_HOUR="1")
    for subject, expected in (("first", 2), ("second", 0)):
        waiter = await _start("wait", env, STOP)
        await _wait_for_cursor(tmp_path / "state")
        await asyncio.sleep(0.5)
        await mcp.send("BlueLake", ["GreenCastle"], subject)
        code, _, _ = await _finish(waiter)
        assert code == expected, subject
    assert "1 wake-ups in the last hour (limit 1)" in (_agent_state(tmp_path / "state") / "wake.log").read_text()


@pytest.mark.asyncio
async def test_sender_allowlist(mcp: Mail,server_url: str, tmp_path: Path) -> None:
    env = _env(server_url, tmp_path / "state", AGENT_MAIL_WAKE_FROM="RedStone")
    waiter = await _start("wait", env, STOP)
    await _wait_for_cursor(tmp_path / "state")
    await mcp.send("BlueLake", ["GreenCastle"], "not on the list")
    await mcp.send("RedStone", ["GreenCastle"], "on the list")
    code, _, stderr = await _finish(waiter)
    assert code == 2
    assert "on the list" in stderr and "not on the list" not in stderr


@pytest.mark.asyncio
async def test_new_session_replaces_a_leftover_waiter(mcp: Mail,server_url: str, tmp_path: Path) -> None:
    state = tmp_path / "state"
    env = _env(server_url, state)
    leftover = await _start("wait", env, {"hook_event_name": "Stop", "session_id": "old-session"})
    await _wait_for_cursor(state)
    await asyncio.sleep(0.5)

    # A waiter from a closed session would announce mail to nobody, so a new session's start replaces it.
    start = {"hook_event_name": "SessionStart", "session_id": "new-session"}
    assert await _run("context", env, start) == (0, "", "")
    assert await _finish(leftover, within=5) == (0, "", "")

    await mcp.send("BlueLake", ["GreenCastle"], "for the new session")
    code, stdout, _ = await _run("context", env, start)
    output = json.loads(stdout)["hookSpecificOutput"]
    assert (code, output["hookEventName"]) == (0, "SessionStart")
    assert '"for the new session"' in output["additionalContext"]


AGY = {"conversationId": "c1", "workspacePaths": ["/repo"], "transcriptPath": "/t.jsonl"}


@pytest.mark.asyncio
async def test_antigravity_context_and_stop(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    env = _env(server_url, tmp_path / "state")
    # The first call only sets the cursor: history is never announced as new.
    await mcp.send("BlueLake", ["GreenCastle"], "history")
    assert await _run("agy-context", env, AGY) == (0, "{}\n", "")

    await mcp.send("BlueLake", ["GreenCastle"], "before the next model call", thread_id="br-9")
    code, stdout, _ = await _run("agy-context", env, AGY)
    message = json.loads(stdout)["injectSteps"][0]["ephemeralMessage"]
    assert code == 0 and '"before the next model call" (thread br-9)' in message

    assert await _run("agy-stop", env, AGY) == (0, "{}\n", "")  # nothing waiting
    await mcp.send("RedStone", ["GreenCastle"], "arrived during the turn")
    code, stdout, _ = await _run("agy-stop", env, AGY)
    answer = json.loads(stdout)
    assert answer["decision"] == "continue" and '"arrived during the turn"' in answer["reason"]


@pytest.mark.asyncio
async def test_antigravity_stop_respects_quota(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    state = tmp_path / "state"
    status = {"quota": {"weekly": {"remaining_fraction": 0.1, "reset_in_seconds": 9000}}, "plan_tier": "pro"}
    code, stdout, _ = await _run("record-usage", _env(server_url, state), status)
    assert (code, stdout.strip()) == (0, "weekly 90%")

    env = _env(server_url, state, AGENT_MAIL_WAKE_MAX_USAGE_PCT="80")
    assert await _run("agy-context", env, AGY) == (0, "{}\n", "")
    await mcp.send("BlueLake", ["GreenCastle"], "over quota")
    assert await _run("agy-stop", env, AGY) == (0, "{}\n", "")
    # Not lost: announced before the next model call instead.
    code, stdout, _ = await _run("agy-context", env, AGY)
    assert '"over quota"' in json.loads(stdout)["injectSteps"][0]["ephemeralMessage"]


def _config_file(server_url: str, tmp_path: Path, **extra: str) -> Path:
    """Settings in a file instead of the environment, the way the token stays out of the model's sight."""
    path = tmp_path / "agent-mail.json"
    path.write_text(json.dumps({
        "AGENT_MAIL_SERVER_URL": server_url,
        "AGENT_MAIL_TOKEN": TOKEN,
        "AGENT_MAIL_PROJECT": PROJECT,
        "AGENT_MAIL_AGENT": "GreenCastle",
        "AGENT_MAIL_WAKE_STATE_DIR": str(tmp_path / "state"),
        **extra,
    }))
    return path


async def _start_args(args: list[str], hook_input: dict[str, Any] | None = None) -> asyncio.subprocess.Process:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(SCRIPT), *args, env={"PATH": os.environ["PATH"]},
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdin is not None
    if hook_input is not None:
        proc.stdin.write(json.dumps(hook_input).encode())
        await proc.stdin.drain()
    proc.stdin.close()
    return proc


async def _wait_for_watcher(state: Path) -> None:
    holder = _agent_state(state) / "lock.holder"
    for _ in range(100):
        if holder.exists() and json.loads(holder.read_text()).get("kind") == "watch" and (
            _agent_state(state) / "cursor.json"
        ).exists():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("watcher never started listening")


@pytest.mark.asyncio
async def test_antigravity_watcher_wakes_the_agent_by_finishing(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    config = _config_file(server_url, tmp_path)
    context = ["agy-context", "--config", str(config)]

    # No watcher yet: the agent is told how to start one, without ever seeing the token.
    code, stdout, _ = await _finish(await _start_args(context, AGY))
    instruction = json.loads(stdout)["injectSteps"][-1]["ephemeralMessage"]
    assert code == 0 and f"watch --config {config}" in instruction and TOKEN not in instruction
    # Reminded once, not before every model call.
    assert await _finish(await _start_args(context, AGY)) == (0, "{}\n", "")

    watcher = await _start_args(["watch", "--config", str(config)])
    await _wait_for_watcher(tmp_path / "state")
    # While it runs, the hooks leave announcing to it.
    await mcp.send("BlueLake", ["GreenCastle"], "for the watcher")
    code, stdout, _ = await _finish(watcher)
    assert code == 0
    assert '"for the watcher"' in stdout
    assert f"start the watcher again as a background task: python3 {SCRIPT} watch --config {config}" in stdout
    assert await _finish(await _start_args(context, AGY)) == (0, "{}\n", "")  # already announced


@pytest.mark.asyncio
async def test_watcher_waits_out_token_limits(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    config = _config_file(server_url, tmp_path, AGENT_MAIL_WAKE_ACCOUNT="agy", AGENT_MAIL_WAKE_MAX_USAGE_PCT="80")
    busy = {"quota": {"weekly": {"remaining_fraction": 0.05}}}
    await _finish(await _start_args(["record-usage", "--config", str(config)], busy))
    await _finish(await _start_args(["agy-context", "--config", str(config)], AGY))

    watcher = await _start_args(["watch", "--config", str(config)])
    await _wait_for_watcher(tmp_path / "state")
    await mcp.send("BlueLake", ["GreenCastle"], "while over quota")
    await asyncio.sleep(2)
    assert watcher.returncode is None  # finishing would wake the agent just to say "not now"

    await _finish(await _start_args(["record-usage", "--config", str(config)], {"quota": {"weekly": {"remaining_fraction": 0.6}}}))
    await mcp.send("RedStone", ["GreenCastle"], "after the quota recovered")
    code, stdout, _ = await _finish(watcher)
    assert code == 0 and "2 new messages" in stdout
    assert '"while over quota"' in stdout and '"after the quota recovered"' in stdout


@pytest.mark.asyncio
async def test_new_conversation_replaces_a_stale_watcher(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    config = _config_file(server_url, tmp_path)
    await _finish(await _start_args(["agy-context", "--config", str(config)], AGY))
    watcher = await _start_args(["watch", "--config", str(config)])
    await _wait_for_watcher(tmp_path / "state")

    other = {**AGY, "conversationId": "c2"}
    code, stdout, _ = await _finish(await _start_args(["agy-context", "--config", str(config)], other))
    assert code == 0 and "watch --config" in stdout  # c2 is asked to start its own
    assert (await _finish(watcher, within=5))[0] == 0


@pytest.mark.asyncio
async def test_unconfigured_hook_is_silent(tmp_path: Path) -> None:
    env = {"PATH": os.environ["PATH"], "AGENT_MAIL_WAKE_STATE_DIR": str(tmp_path / "state")}
    assert await _run("wait", env, STOP) == (0, "", "")
    assert await _run("context", env, {"hook_event_name": "SessionStart"}) == (0, "", "")
    assert await _run("agy-stop", env, AGY) == (0, "{}\n", "")
