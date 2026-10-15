"""Personal access tokens: minted by `mail_wake.py login` through the browser, used instead of the shared token."""

from __future__ import annotations

import asyncio
import json
import os
import stat
import subprocess
import sys
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from mcp_agent_mail import config as _config, ui_auth
from mcp_agent_mail.db import ensure_schema, get_session
from mcp_agent_mail.http import build_http_app
from tests.live_server import TOKEN as LIVE_TOKEN

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


def test_cli_login_end_to_end(server_url: str, tmp_path: Path) -> None:
    browser, log = tmp_path / "fake-browser", tmp_path / "browser.log"
    browser.write_text(FAKE_BROWSER.format(python=sys.executable, token=LIVE_TOKEN, log=log))
    browser.chmod(0o755)
    config = tmp_path / "config" / "repo.json"
    try:
        done = subprocess.run(
            [sys.executable, str(SCRIPT), "login", "--config", str(config), "--server", server_url,
             "--project", "github.com/owner/repo", "--agent", "BlueLake"],
            env={"PATH": os.environ["PATH"], "BROWSER": str(browser)}, capture_output=True, text=True, timeout=60,
        )
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
