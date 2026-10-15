"""Per-IP sign-in throttling and temporary bans (ip_guard.py)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from typer.testing import CliRunner

from mcp_agent_mail import audit, config as _config, ip_guard, ui_auth
from mcp_agent_mail.cli import app as cli_app
from mcp_agent_mail.config import get_settings
from mcp_agent_mail.db import ensure_schema
from mcp_agent_mail.http import build_http_app

TOKEN = "vps-secret-token"
PASSWORD = "correct horse battery staple"


@pytest.fixture
async def make_client(isolated_env, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    monkeypatch.setenv("HTTP_AUTH_THROTTLE_EXEMPT_IPS", "198.51.100.77")
    _config.clear_settings_cache()
    ip_guard._ban_cache.clear()
    await ensure_schema()
    app = build_http_app(get_settings())
    clients: list[AsyncClient] = []

    def make(ip: str) -> AsyncClient:
        client = AsyncClient(transport=ASGITransport(app=app, client=(ip, 40000)), base_url="https://am.example")
        clients.append(client)
        return client

    yield make
    for client in clients:
        await client.aclose()


async def _token_sign_in(client: AsyncClient, token: str = TOKEN) -> int:
    return (await client.post("/mail/login", data={"token": token, "next": "/mail"})).status_code


async def _api(client: AsyncClient, token: str) -> int:
    response = await client.get("/mail/api/events", params={"timeout": 0.1}, headers={"Authorization": f"Bearer {token}"})
    return response.status_code


@pytest.mark.asyncio
async def test_many_failed_sign_ins_pause_that_address_only(make_client) -> None:
    attacker, colleague = make_client("203.0.113.5"), make_client("203.0.113.6")
    await ui_auth.create_user("alice", PASSWORD)
    # One password tried across many usernames: no single account ever locks.
    for i in range(ip_guard.SIGNIN_LIMIT):
        response = await attacker.post("/mail/auth/password", data={"username": f"user{i}", "password": "Summer2026!"})
        assert response.status_code == 401
    paused = await attacker.post("/mail/auth/password", data={"username": "alice", "password": PASSWORD})
    assert paused.status_code == 429 and "Too many failed sign-ins" in paused.text and paused.headers["Retry-After"]
    assert await _token_sign_in(attacker) == 429  # every sign-in method, even with the right credential

    assert await _token_sign_in(colleague) == 303  # other addresses are unaffected
    assert ("anonymous", "auth.signin_throttled") in [(e.actor, e.action) for e in await audit.recent()]


@pytest.mark.asyncio
async def test_credential_guessing_bans_the_address(make_client) -> None:
    scanner = make_client("203.0.113.9")
    for _ in range(ip_guard.CREDENTIAL_LIMIT):
        assert await _api(scanner, "guess") == 401
    banned = await _api(scanner, TOKEN)  # banned now, even with the real token
    assert banned == 403
    assert (await scanner.get("/mail/login")).status_code == 403
    assert (await scanner.get("/health/liveness")).status_code == 200  # health checks stay open
    [entry] = [e for e in await audit.recent() if e.action == "ip.banned"]
    assert entry.target_id == "203.0.113.9"
    assert await _api(make_client("203.0.113.10"), TOKEN) == 200

    assert [b.ip for b in await ip_guard.list_bans()] == ["203.0.113.9"]
    assert await ip_guard.clear("203.0.113.9")
    assert await _api(scanner, TOKEN) == 200


@pytest.mark.asyncio
async def test_browsing_never_bans_your_own_address(make_client) -> None:
    """Only rejected Authorization headers count: not a browser's favicon request, and not a
    Live page left open after its session ended (it keeps polling with a stale cookie)."""
    browser = make_client("203.0.113.30")
    for _ in range(ip_guard.CREDENTIAL_LIMIT + 5):
        assert (await browser.get("/favicon.ico")).status_code == 204  # public now, and not a 401
        assert (await browser.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401  # no credential
        stale = await browser.get("/mail/api/events", params={"timeout": 0.1}, cookies={"agent_mail_ui": "expired-session"})
        assert stale.status_code == 401
    assert await _api(browser, TOKEN) == 200  # not banned
    assert (await browser.get("/favicon.svg")).headers["content-type"].startswith("image/svg+xml")


def test_bans_cli(isolated_env) -> None:
    import asyncio

    async def ban() -> None:
        for _ in range(ip_guard.CREDENTIAL_LIMIT):
            await ip_guard.record_failure("203.0.113.20", "credential")

    asyncio.run(ban())
    runner = CliRunner()
    listed = runner.invoke(cli_app, ["bans", "list"])
    assert listed.exit_code == 0 and "203.0.113.20" in listed.output
    cleared = runner.invoke(cli_app, ["bans", "clear", "203.0.113.20"])
    assert cleared.exit_code == 0 and "Cleared" in cleared.output
    assert "No active bans" in runner.invoke(cli_app, ["bans", "list"]).output
    assert [(e.actor, e.action) for e in asyncio.run(audit.recent())] == [("cli", "ip.unbanned")]


@pytest.mark.asyncio
async def test_exempt_loopback_and_untrusted_proxies_are_never_throttled(make_client) -> None:
    for ip in ("198.51.100.77", "127.0.0.1"):  # HTTP_AUTH_THROTTLE_EXEMPT_IPS, and loopback
        client = make_client(ip)
        for _ in range(ip_guard.CREDENTIAL_LIMIT + 5):
            await _api(client, "guess")
        assert await _api(client, TOKEN) == 200

    # A proxy whose forwarding headers uvicorn didn't trust: every client would look like the proxy,
    # so a ban would lock everyone out. Throttling stands down instead.
    proxy = make_client("172.18.0.1")
    for _ in range(ip_guard.CREDENTIAL_LIMIT + 5):
        response = await proxy.get(
            "/mail/api/events", params={"timeout": 0.1},
            headers={"Authorization": "Bearer guess", "X-Forwarded-For": "203.0.113.50"},
        )
        assert response.status_code == 401
    assert await ip_guard.list_bans() == []
