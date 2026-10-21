"""Personal access tokens: minted by `mail_wake.py login` through the browser, used instead of the shared token."""

from __future__ import annotations

import asyncio
import json
import os
import stat
import subprocess
import sys
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError
from httpx import ASGITransport, AsyncClient
from mcp.types import TextResourceContents
from sqlalchemy import text

from mcp_agent_mail import config as _config, ui_auth
from mcp_agent_mail.db import ensure_schema, get_session
from mcp_agent_mail.http import build_http_app
from tests.live_server import PROJECT as LIVE_PROJECT, TOKEN as LIVE_TOKEN

TOKEN = "vps-secret-token"
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "hooks" / "mail_wake.py"
STATE = "s" * 24


@pytest.fixture
async def app_client(isolated_env, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    _config.clear_settings_cache()
    ui_auth._token_cache.clear()
    await ensure_schema()
    async with AsyncClient(transport=ASGITransport(app=build_http_app(_config.get_settings())), base_url="https://am.example") as c:
        yield c


async def _sign_in(client: AsyncClient) -> None:
    assert (await client.post("/mail/login", data={"token": TOKEN, "next": "/mail"})).status_code == 303


async def _mint_via_consent(client: AsyncClient, name: str = "peters-mac · ldm") -> str:
    consent = await client.get("/mail/auth/cli", params={"port": "54321", "state": STATE, "name": name})
    assert consent.status_code == 200 and name in consent.text
    approved = await client.post("/mail/auth/cli", data={"port": "54321", "state": STATE, "name": name})
    assert approved.status_code == 303
    location = urlparse(approved.headers["location"])
    assert (location.scheme, location.netloc, location.path) == ("http", "127.0.0.1:54321", "/callback")
    query = parse_qs(location.query)
    assert query["state"] == [STATE]
    return query["token"][0]


async def _api(client: AsyncClient, token: str) -> int:
    """Status of an API call made the way an agent makes it: the token, and no browser session cookie."""
    session_cookies = client.cookies
    client.cookies = httpx.Cookies()
    try:
        response = await client.get(
            "/mail/api/events", params={"timeout": 0.1}, headers={"Authorization": f"Bearer {token}"}
        )
    finally:
        client.cookies = session_cookies
    return response.status_code


@pytest.mark.asyncio
async def test_consent_needs_a_signed_in_person(app_client: AsyncClient) -> None:
    start = await app_client.get("/mail/auth/cli", params={"port": "54321", "state": STATE, "name": "laptop"})
    assert start.status_code == 303 and start.headers["location"].startswith("/mail/login?next=%2Fmail%2Fauth%2Fcli")
    # A POST from another site carries no session cookie, so nothing is minted.
    forged = await app_client.post("/mail/auth/cli", data={"port": "54321", "state": STATE, "name": "x"})
    assert forged.status_code == 303 and forged.headers["location"] == "/mail/login"
    async with get_session() as session:
        assert (await session.execute(text("SELECT COUNT(*) FROM api_tokens"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_token_works_like_the_shared_one_and_is_stored_hashed(app_client: AsyncClient) -> None:
    await _sign_in(app_client)
    token = await _mint_via_consent(app_client)
    assert token.startswith("amt_")
    app_client.cookies.clear()  # an agent has no browser session
    assert await _api(app_client, token) == 200
    assert await _api(app_client, token + "x") == 401
    mcp = await app_client.post(
        "/mcp/", json={"jsonrpc": "2.0", "id": "1", "method": "tools/list"},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json, text/event-stream"},
    )
    assert mcp.status_code != 401

    async with get_session() as session:
        stored = (await session.execute(text("SELECT owner, token_hash, prefix, last_used_ts FROM api_tokens"))).one()
    assert stored[0] == "_token" and token not in stored[1] and token.startswith(stored[2])
    assert stored[3] is not None  # last used is recorded


@pytest.mark.asyncio
async def test_revoked_and_expired_tokens_stop_working(app_client: AsyncClient) -> None:
    await _sign_in(app_client)
    first, second = await _mint_via_consent(app_client, "one"), await _mint_via_consent(app_client, "two")
    page = await app_client.get("/mail/account/tokens")
    assert page.status_code == 200 and "one" in page.text and first[:10] in page.text and first not in page.text

    async with get_session() as session:
        first_id = (await session.execute(text("SELECT id FROM api_tokens WHERE name = 'one'"))).scalar_one()
    assert (await app_client.post(f"/mail/account/tokens/{first_id}/revoke")).status_code == 303
    assert await _api(app_client, first) == 401
    assert "revoked" in (await app_client.get("/mail/account/tokens")).text

    async with get_session() as session:
        await session.execute(text("UPDATE api_tokens SET expires_ts = :t WHERE name = 'two'"),
                              {"t": ui_auth._now() - timedelta(seconds=1)})
        await session.commit()
    ui_auth._token_cache.clear()
    assert await _api(app_client, second) == 401


@pytest.mark.asyncio
async def test_people_only_manage_their_own_tokens(app_client: AsyncClient) -> None:
    await ui_auth.create_user("someone", "correct horse battery staple")  # a real person who may sign in
    token, row = await ui_auth.create_api_token(f"{ui_auth.SESSION_PREFIX}someone", "their laptop")
    await _sign_in(app_client)
    assert (await app_client.post(f"/mail/account/tokens/{row.id}/revoke")).status_code == 404
    assert "their laptop" not in (await app_client.get("/mail/account/tokens")).text
    app_client.cookies.clear()
    assert await _api(app_client, token) == 200


@pytest.mark.asyncio
async def test_a_token_can_be_issued_for_one_agent(app_client: AsyncClient) -> None:
    """#42: login names the agent; the person sees it before approving, and the token records it."""
    await _sign_in(app_client)
    agent = {"project": "github.com/owner/repo", "agent": "GreenHarbor"}
    consent = await app_client.get("/mail/auth/cli", params={"port": "54321", "state": STATE, "name": "fedora", **agent})
    assert consent.status_code == 200 and "GreenHarbor" in consent.text and "github.com/owner/repo" in consent.text
    approved = await app_client.post("/mail/auth/cli", data={"port": "54321", "state": STATE, "name": "fedora", **agent})
    assert approved.status_code == 303
    async with get_session() as session:
        stored = (await session.execute(text("SELECT project_key, agent_name FROM api_tokens"))).one()
    assert tuple(stored) == ("github.com/owner/repo", "GreenHarbor")
    assert "acts as GreenHarbor in github.com/owner/repo" in (await app_client.get("/mail/account/tokens")).text

    # Half a binding, or an agent name that isn't one, is refused rather than ignored.
    for bad in ({"project": "github.com/owner/repo"}, {"agent": "GreenHarbor"}, {**agent, "agent": "Green Harbor"}):
        assert (await app_client.get("/mail/auth/cli", params={"port": "54321", "state": STATE, **bad})).status_code == 400


@pytest.mark.asyncio
async def test_no_listener_shows_the_token_once_and_bad_requests_are_refused(app_client: AsyncClient) -> None:
    await _sign_in(app_client)
    shown = await app_client.post("/mail/auth/cli", data={"state": STATE, "name": "over ssh"})
    assert shown.status_code == 200 and "amt_" in shown.text
    for params in ({"port": "80", "state": STATE}, {"port": "54321", "state": "short"}, {"port": "x", "state": STATE}):
        assert (await app_client.get("/mail/auth/cli", params=params)).status_code == 400


FAKE_BROWSER = r'''#!{python}
"""Plays the person: sign in with the server token, approve, follow the redirect back to the CLI."""
import http.cookiejar, sys, traceback, urllib.error, urllib.parse, urllib.request

sys.stderr = open("{log}", "a")
print("opened with", sys.argv, file=sys.stderr, flush=True)
url = sys.argv[1]
base = url.split("/mail/")[0]
jar = http.cookiejar.CookieJar()

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), NoRedirect)
def post(path, fields):
    try:
        return opener.open(base + path, urllib.parse.urlencode(fields).encode())
    except urllib.error.HTTPError as exc:
        return exc
post("/mail/login", {{"token": "{token}", "next": "/mail"}})
query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
try:
    approved = post("/mail/auth/cli", query)
    print("approve ->", approved.status, approved.headers.get("Location"), file=sys.stderr, flush=True)
    urllib.request.urlopen(approved.headers["Location"]).read()
except Exception:
    traceback.print_exc()
'''


@pytest.mark.asyncio
async def test_cli_login_end_to_end(server_url: str, tmp_path: Path) -> None:
    browser, log = tmp_path / "fake-browser", tmp_path / "browser.log"
    browser.write_text(FAKE_BROWSER.format(python=sys.executable, token=LIVE_TOKEN, log=log))
    browser.chmod(0o755)
    config = tmp_path / "config" / "repo.json"

    def login() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "login", "--config", str(config), "--server", server_url,
             "--project", "github.com/owner/repo", "--agent", "BlueLake"],
            env={"PATH": os.environ["PATH"], "BROWSER": str(browser)}, capture_output=True, text=True, timeout=60,
        )

    try:
        done = await asyncio.to_thread(login)
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(f"login never completed; browser log:\n{log.read_text() if log.exists() else '(browser never ran)'}") from exc
    assert done.returncode == 0, done.stderr + done.stdout
    saved = json.loads(config.read_text())
    assert saved["AGENT_MAIL_TOKEN"].startswith("amt_") and saved["AGENT_MAIL_TOKEN"] != LIVE_TOKEN
    assert (saved["AGENT_MAIL_SERVER_URL"], saved["AGENT_MAIL_PROJECT"], saved["AGENT_MAIL_AGENT"]) == (
        server_url, "github.com/owner/repo", "BlueLake"
    )
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    assert "Signed in" in done.stdout and saved["AGENT_MAIL_TOKEN"] not in done.stdout
    # The token was issued for the agent the machine was set up as (#42).
    identity = await ui_auth.api_token_identity(saved["AGENT_MAIL_TOKEN"])
    assert identity is not None and (identity.project_key, identity.agent_name) == ("github.com/owner/repo", "BlueLake")


@pytest.mark.asyncio
async def test_cli_login_without_a_browser(server_url: str, tmp_path: Path) -> None:
    token, _ = await ui_auth.create_api_token("_token", "pasted")  # what the approval page would show
    config = tmp_path / "repo.json"
    config.write_text(json.dumps({"AGENT_MAIL_SERVER_URL": server_url, "AGENT_MAIL_AGENT": "kept"}))
    def login() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "login", "--config", str(config), "--no-browser"],
            input=token + "\n", env={"PATH": os.environ["PATH"]}, capture_output=True, text=True, timeout=60,
        )

    done = await asyncio.to_thread(login)
    assert done.returncode == 0, done.stderr
    assert "/mail/auth/cli?" in done.stdout
    saved = json.loads(config.read_text())
    assert saved["AGENT_MAIL_TOKEN"] == token and saved["AGENT_MAIL_AGENT"] == "kept"


@asynccontextmanager
async def _session(server_url: str, token: str) -> AsyncGenerator[Client]:
    """A new MCP session, as a restarted agent opens one: only the token in its connection."""
    transport = StreamableHttpTransport(f"{server_url}/mcp/", headers={"Authorization": f"Bearer {token}"})
    async with Client(transport) as client:
        yield client


async def _inbox(client: Client, agent: str, **extra: str) -> list[dict]:
    result = await client.call_tool("fetch_inbox", {"project_key": LIVE_PROJECT, "agent_name": agent, **extra})
    return list(result.structured_content["result"]) if result.structured_content else list(result.data or [])


async def _refused(call: Awaitable[object]) -> str:
    with pytest.raises(ToolError) as refused:
        await call
    return str(refused.value)


@pytest.mark.asyncio
async def test_a_restarted_session_keeps_its_agent_through_its_token(server_url: str) -> None:
    """#42, against a real server: a session holding only the agent's own token is that agent.

    The registration token used to live only in the model's context, so a restart lost the
    agent for good: no inbox, no thread, no reply, and its name couldn't even be registered again.
    """
    await ui_auth.create_user("someone", "correct horse battery staple")  # a second person
    someone = f"{ui_auth.SESSION_PREFIX}someone"
    blue, _ = await ui_auth.create_api_token("_token", "mac", project_key=LIVE_PROJECT, agent_name="BlueLake")
    green, _ = await ui_auth.create_api_token(someone, "theirs", project_key=LIVE_PROJECT, agent_name="GreenCastle")
    blue_for_someone, _ = await ui_auth.create_api_token(someone, "x", project_key=LIVE_PROJECT, agent_name="BlueLake")
    unbound, _ = await ui_auth.create_api_token("_token", "web ui")
    register = {"project_key": LIVE_PROJECT, "program": "claude-code", "model": "opus"}

    async with _session(server_url, blue) as first:
        await first.call_tool("ensure_project", {"human_key": LIVE_PROJECT})
        await first.call_tool("register_agent", {**register, "name": "BlueLake"})
        # Contact approval is not what this test is about.
        await first.call_tool("set_contact_policy", {"project_key": LIVE_PROJECT, "agent_name": "BlueLake", "policy": "open"})
    async with _session(server_url, green) as theirs:
        await theirs.call_tool("register_agent", {**register, "name": "GreenCastle"})
        await theirs.call_tool("send_message", {
            "project_key": LIVE_PROJECT, "sender_name": "GreenCastle", "to": ["BlueLake"],
            "subject": "job", "body_md": "run it", "thread_id": "ldm-verify-test",
        })

    # The restart: a new session, no registration token anywhere.
    async with _session(server_url, blue) as restarted:
        inbox = await _inbox(restarted, "BlueLake")
        assert [m["subject"] for m in inbox] == ["job"]
        thread = await restarted.read_resource(
            f"resource://thread/ldm-verify-test?project={LIVE_PROJECT}&include_bodies=true"
        )
        assert isinstance(thread[0], TextResourceContents) and "run it" in thread[0].text
        await restarted.call_tool("reply_message", {
            "project_key": LIVE_PROJECT, "message_id": inbox[0]["id"], "sender_name": "BlueLake", "body_md": "done",
        })
        again = await restarted.call_tool("register_agent", {**register, "name": "BlueLake"})
        assert again.data["name"] == "BlueLake"
        # It can't reach someone else's agent with its own token.
        assert "GreenCastle" in await _refused(_inbox(restarted, "GreenCastle"))

    # A token issued for BlueLake to someone who doesn't own BlueLake, and a token issued for no
    # agent, are refused exactly as before.
    for token in (blue_for_someone, unbound):
        async with _session(server_url, token) as other:
            assert "BlueLake" in await _refused(_inbox(other, "BlueLake"))


@pytest.mark.asyncio
async def test_an_agent_from_before_owners_is_claimed_by_proving_it(server_url: str) -> None:
    """#42: agents made before owners were recorded (or with the shared token) have no owner. A
    token issued for one can't take it over; it must prove the agent once, which claims it."""
    register = {"project_key": LIVE_PROJECT, "program": "claude-code", "model": "opus", "name": "RedStone"}
    async with _session(server_url, LIVE_TOKEN) as shared:  # the shared server token: no person
        await shared.call_tool("ensure_project", {"human_key": LIVE_PROJECT})
        registration_token = (await shared.call_tool("register_agent", register)).data["registration_token"]
    token, _ = await ui_auth.create_api_token("_token", "mac", project_key=LIVE_PROJECT, agent_name="RedStone")

    async with _session(server_url, token) as before_proof:
        assert "RedStone" in await _refused(_inbox(before_proof, "RedStone"))
        await _inbox(before_proof, "RedStone", registration_token=registration_token)  # the proof claims it
    async with _session(server_url, token) as restarted:
        assert await _inbox(restarted, "RedStone") == []
    async with get_session() as session:
        owner = (await session.execute(text("SELECT owner FROM agents WHERE name = 'RedStone'"))).scalar_one()
    assert owner == "_token"
