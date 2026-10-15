"""Push delivery: scripts/hooks/mail_wake.py against a real server process over HTTP."""

from __future__ import annotations

import asyncio
import json
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from tests.live_server import PROJECT, TOKEN, Mail, client_env

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "hooks" / "mail_wake.py"


def _env(server_url: str, state: Path, **overrides: str) -> dict[str, str]:
    env = client_env(
        AGENT_MAIL_SERVER_URL=server_url,
        AGENT_MAIL_TOKEN=TOKEN,
        AGENT_MAIL_PROJECT=PROJECT,
        AGENT_MAIL_AGENT="GreenCastle",
        AGENT_MAIL_WAKE_STATE_DIR=str(state),
        AGENT_MAIL_WAKE_WAIT_SECONDS="20",
    )
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


def _presence_of(server_url: str, agent: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{server_url}/mail/api/presence?project={PROJECT}&include_offline=true",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return next(a for a in json.load(response)["agents"] if a["agent"] == agent)


@pytest.mark.asyncio
async def test_wait_wakes_only_for_mail_addressed_to_the_agent(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    await mcp.send("BlueLake", ["GreenCastle"], "old mail, before the waiter")
    env = _env(server_url, tmp_path / "state", AGENT_MAIL_MACHINE="peters-mac")
    waiter = await _start("wait", env, STOP)
    await _wait_for_cursor(tmp_path / "state")
    presence = _presence_of(server_url, "GreenCastle")
    assert (presence["status"], presence["wakeable"], presence["machine"], presence["client"]) == (
        "idle", True, "peters-mac", "claude-code"
    )

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
    presence = _presence_of(server_url, "GreenCastle")
    assert (presence["status"], presence["wakeable"]) == ("busy", False)  # woken to handle the mail


@pytest.mark.asyncio
async def test_only_one_waiter_per_agent_and_timeout_is_silent(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    env = _env(server_url, tmp_path / "state", AGENT_MAIL_WAKE_WAIT_SECONDS="3")
    first = await _start("wait", env, STOP)
    await _wait_for_cursor(tmp_path / "state")

    assert await _run("wait", env, STOP) == (0, "", "")  # a second Stop while the first waits
    assert await _finish(first) == (0, "", "")  # no mail: times out without waking


@pytest.mark.asyncio
async def test_usage_limit_holds_mail_for_the_next_prompt(mcp: Mail, server_url: str, tmp_path: Path) -> None:
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
async def test_wake_cap_per_hour(mcp: Mail, server_url: str, tmp_path: Path) -> None:
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
async def test_sender_allowlist(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    env = _env(server_url, tmp_path / "state", AGENT_MAIL_WAKE_FROM="RedStone")
    waiter = await _start("wait", env, STOP)
    await _wait_for_cursor(tmp_path / "state")
    await mcp.send("BlueLake", ["GreenCastle"], "not on the list")
    await mcp.send("RedStone", ["GreenCastle"], "on the list")
    code, _, stderr = await _finish(waiter)
    assert code == 2
    assert "on the list" in stderr and "not on the list" not in stderr


@pytest.mark.asyncio
async def test_new_session_replaces_a_leftover_waiter(mcp: Mail, server_url: str, tmp_path: Path) -> None:
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
        sys.executable, str(SCRIPT), *args, env=client_env(),
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
    env = client_env(AGENT_MAIL_WAKE_STATE_DIR=str(tmp_path / "state"))
    assert await _run("wait", env, STOP) == (0, "", "")
    assert await _run("context", env, {"hook_event_name": "SessionStart"}) == (0, "", "")
    assert await _run("agy-stop", env, AGY) == (0, "{}\n", "")


# ---------------------------------------------------------------- client versions (#12)

CURRENT = re.findall(r'^CLIENT_VERSION = "([0-9.]+)"', SCRIPT.read_text(), re.MULTILINE)[0]


def _old_client(tmp_path: Path, version: str = "1.0.0") -> Path:
    """A copy of the client scripts that reports an older CLIENT_VERSION, as on a machine nobody updated."""
    folder = tmp_path / "old-client"
    folder.mkdir()
    for name in ("mail_wake.py", "agent_mail_files.py"):
        text = (SCRIPT.parent / name).read_text()
        (folder / name).write_text(text.replace(f'CLIENT_VERSION = "{CURRENT}"', f'CLIENT_VERSION = "{version}"'))
    return folder / "mail_wake.py"


async def _run_script(script: Path, args: list[str], env: dict[str, str], hook_input: dict[str, Any] | None = None) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(script), *args, env=env,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(json.dumps(hook_input or {}).encode()), 30)
    assert proc.returncode is not None
    return proc.returncode, stdout.decode(), stderr.decode()


def _context_text(stdout: str) -> str:
    return json.loads(stdout)["hookSpecificOutput"]["additionalContext"] if stdout.strip() else ""


PROMPT = {"hook_event_name": "UserPromptSubmit", "session_id": "s1"}


def _legacy_heartbeat(server_url: str) -> dict[str, Any]:
    """A heartbeat from a script from before versioning: recognised by its User-Agent, no version header."""
    request = urllib.request.Request(
        f"{server_url}/mail/api/presence",
        data=json.dumps({"project": PROJECT, "agent": "GreenCastle", "state": "idle", "client": "antigravity"}).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}",
                 "User-Agent": "mcp_agent_mail-wake (+https://github.com/Dicklesworthstone/mcp_agent_mail)"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


@pytest.mark.asyncio
async def test_presence_shows_which_machines_run_outdated_clients(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    assert _legacy_heartbeat(server_url)["client_latest"] == CURRENT
    presence = _presence_of(server_url, "GreenCastle")
    assert presence["client_version"] == "" and presence["client_outdated"] is True
    tool = await mcp.client.call_tool("who_is_online", {"project_key": PROJECT})
    assert [a["client_outdated"] for a in tool.data["agents"] if a["agent"] == "GreenCastle"] == [True]

    code, stdout, _ = await _run("context", _env(server_url, tmp_path / "state"), PROMPT)
    assert code == 0 and "newer client" not in stdout
    presence = _presence_of(server_url, "GreenCastle")
    assert presence["client_version"] == CURRENT and presence["client_outdated"] is False
    # Agents without hooks have no client to be out of date.
    assert _presence_of(server_url, "BlueLake")["client_outdated"] is False


@pytest.mark.asyncio
async def test_old_client_is_told_once_a_day_and_updates_itself(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    old = _old_client(tmp_path)
    env = _env(server_url, tmp_path / "state")
    code, stdout, _ = await _run_script(old, ["context"], env, PROMPT)
    text = _context_text(stdout)
    assert code == 0 and f"newer client ({CURRENT}) than this machine's (1.0.0)" in text
    assert f"{old} update" in text and "Ask the user" in text
    assert _context_text((await _run_script(old, ["context"], env, PROMPT))[1]) == ""  # not again today

    code, stdout, stderr = await _run_script(old, ["update", "--server", server_url], env)
    assert code == 0, stderr
    assert f"from 1.0.0 to {CURRENT}" in stdout
    for name in ("mail_wake.py", "agent_mail_files.py"):
        assert (old.parent / name).read_text() == (SCRIPT.parent / name).read_text()
    code, stdout, _ = await _run_script(old, ["update", "--server", server_url], env)
    assert code == 0 and "Already up to date" in stdout


@pytest.mark.asyncio
async def test_update_refuses_plain_http_to_a_remote_server(tmp_path: Path) -> None:
    old = _old_client(tmp_path)
    before = old.read_text()
    code, _, stderr = await _run_script(old, ["update", "--server", "http://agentmail.example.com"], client_env())
    assert code == 1 and "use https" in stderr and old.read_text() == before


@pytest.fixture
def minimum_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIENT_MIN_VERSION", "99.0")


@pytest.mark.asyncio
async def test_server_minimum_refuses_old_clients_and_says_how_to_update(
    minimum_version: None, mcp: Mail, server_url: str, tmp_path: Path
) -> None:
    # MCP calls and browsers aren't client scripts: unaffected (the mcp fixture just registered agents).
    config = _config_file(server_url, tmp_path)
    code, stdout, _ = await _start_and_finish(["context", "--config", str(config)], PROMPT)
    text = _context_text(stdout)
    assert code == 0 and "older than the server's minimum (99.0)" in text and "mail can't reach you" in text
    assert f"update --config {config}" in text
    # Every prompt, not once a day: it can't receive mail until updated.
    assert "minimum (99.0)" in _context_text((await _start_and_finish(["context", "--config", str(config)], PROMPT))[1])

    code, _, _ = await _start_and_finish(["wait", "--config", str(config)], STOP)  # gives up at once
    assert code == 0
    code, stdout, _ = await _start_and_finish(["watch", "--config", str(config)])
    assert code == 0 and "minimum (99.0)" in stdout
    code, stdout, _ = await _start_and_finish(["agy-context", "--config", str(config)], {"conversationId": "c1"})
    messages = [s["ephemeralMessage"] for s in json.loads(stdout)["injectSteps"]]
    assert any("minimum (99.0)" in m for m in messages) and not any("background task" in m for m in messages)


async def _start_and_finish(args: list[str], hook_input: dict[str, Any] | None = None) -> tuple[int, str, str]:
    return await _finish(await _start_args(args, hook_input))
