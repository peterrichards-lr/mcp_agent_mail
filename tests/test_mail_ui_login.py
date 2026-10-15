"""Browser login for the /mail UI on a server that requires a bearer token (e.g. on a VPS)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from mcp_agent_mail import config as _config, http as http_module, ui_auth
from mcp_agent_mail.db import get_session
from mcp_agent_mail.http import UI_SESSION_COOKIE, build_http_app

TOKEN = "vps-secret-token"


async def _client(
    monkeypatch: pytest.MonkeyPatch, *, jwt: bool = False, github: bool = False, token_login: bool = True
) -> AsyncClient:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    monkeypatch.setenv("HTTP_UI_TOKEN_LOGIN", "true" if token_login else "false")
    if jwt:
        monkeypatch.setenv("HTTP_JWT_ENABLED", "true")
        monkeypatch.setenv("HTTP_JWT_SECRET", "jwt-secret")
    if github:
        monkeypatch.setenv("GITHUB_OAUTH_CLIENT_ID", "gh-client")
        monkeypatch.setenv("GITHUB_OAUTH_CLIENT_SECRET", "gh-secret")
        monkeypatch.setenv("GITHUB_ALLOWED_USERS", "PeterRichards-LR, colleague")
    _config.clear_settings_cache()
    app = build_http_app(_config.get_settings())
    return AsyncClient(transport=ASGITransport(app=app), base_url="https://agent-mail.example")


@pytest.fixture
async def client(isolated_env, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    async with await _client(monkeypatch) as c:
        yield c


async def _login(client: AsyncClient, token: str = TOKEN, next_path: str = "/mail/live"):
    return await client.post("/mail/login", data={"token": token, "next": next_path})


@pytest.mark.asyncio
async def test_page_load_redirects_to_login_but_api_calls_get_401(client: AsyncClient) -> None:
    page = await client.get("/mail/live?project=alpha", headers={"accept": "text/html"})
    assert page.status_code == 303
    assert page.headers["location"] == "/mail/login?next=%2Fmail%2Flive%3Fproject%3Dalpha"

    api = await client.get("/mail/api/events", params={"timeout": 0.1})
    assert api.status_code == 401

    form = await client.get("/mail/login", params={"next": "/mail/live"})
    assert form.status_code == 200
    assert 'name="token"' in form.text


@pytest.mark.asyncio
async def test_wrong_token_is_rejected_without_a_cookie(client: AsyncClient) -> None:
    response = await _login(client, token="guess")
    assert response.status_code == 401
    assert "not valid" in response.text
    assert UI_SESSION_COOKIE not in response.cookies


@pytest.mark.asyncio
async def test_login_cookie_opens_ui_and_event_stream(client: AsyncClient) -> None:
    response = await _login(client)
    assert response.status_code == 303
    assert response.headers["location"] == "/mail/live"
    set_cookie = response.headers["set-cookie"].lower()
    for attribute in ("httponly", "samesite=lax", "secure", "path=/mail"):
        assert attribute in set_cookie
    assert TOKEN not in response.headers["set-cookie"]

    assert (await client.get("/mail/live")).status_code == 200
    stream = await client.get("/mail/api/events", params={"timeout": 0.1})
    assert stream.status_code == 200
    # The cookie is scoped to the UI; it is not a credential for the MCP endpoint.
    assert (await client.post("/mcp/", json={"jsonrpc": "2.0", "id": "1", "method": "tools/list"})).status_code == 401

    await client.post("/mail/logout")
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401


@pytest.mark.asyncio
async def test_forged_idle_and_revoked_sessions_are_rejected(client: AsyncClient) -> None:
    await _login(client)
    real = client.cookies[UI_SESSION_COOKIE]
    assert len(real) >= 40 and TOKEN not in real  # a random session token, nothing derived from the bearer token

    client.cookies.clear()
    client.cookies.set(UI_SESSION_COOKIE, real[:-1] + ("A" if real[-1] != "A" else "B"), path="/mail")
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401

    # Idle past the limit: the session is gone even though the browser still holds its cookie.
    async with get_session() as session:
        await session.execute(text("UPDATE ui_sessions SET last_seen_ts = :t"), {"t": ui_auth._now() - timedelta(days=31)})
        await session.commit()
    ui_auth._session_cache.clear()
    client.cookies.clear()
    client.cookies.set(UI_SESSION_COOKIE, real, path="/mail")
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401

    # Signing out ends the session on the server, not just in this browser.
    client.cookies.clear()
    await _login(client)
    copied = client.cookies[UI_SESSION_COOKIE]
    await client.post("/mail/logout")
    client.cookies.set(UI_SESSION_COOKIE, copied, path="/mail")
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401


async def _session_users() -> list[str]:
    async with get_session() as session:
        return [r[0] for r in (await session.execute(text("SELECT user FROM ui_sessions WHERE revoked_ts IS NULL"))).all()]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["https://evil.example/", "//evil.example/mail", "/mail\\..\\x", "/other"])
async def test_login_never_redirects_off_the_ui(client: AsyncClient, target: str) -> None:
    response = await _login(client, next_path=target)
    assert response.headers["location"] == "/mail"


@pytest.mark.asyncio
async def test_cookie_also_satisfies_jwt_middleware(isolated_env, monkeypatch: pytest.MonkeyPatch) -> None:
    async with await _client(monkeypatch, jwt=True) as client:
        assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401
        assert (await _login(client)).status_code == 303
        assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 200


# ----- Sign in with GitHub -----


def _fake_github(monkeypatch: pytest.MonkeyPatch, login: str, seen: list[httpx.Request]) -> None:
    """Replace github.com / api.github.com with a fake that signs `login` in."""

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/login/oauth/access_token":
            form = parse_qs(request.content.decode())
            if form.get("code") != ["good-code"] or form.get("client_secret") != ["gh-secret"]:
                return httpx.Response(200, json={"error": "bad_verification_code"})
            return httpx.Response(200, json={"access_token": "gh-user-token", "token_type": "bearer"})
        if request.url.path == "/user":
            assert request.headers["Authorization"] == "Bearer gh-user-token"
            return httpx.Response(200, json={"login": login, "id": 1})
        return httpx.Response(404)

    monkeypatch.setattr(
        http_module, "_github_http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


async def _start_github(client: AsyncClient, next_path: str = "/mail/live") -> str:
    """Begin GitHub sign-in; return the state sent to GitHub."""
    response = await client.get("/mail/auth/github", params={"next": next_path})
    assert response.status_code == 303
    location = urlparse(response.headers["location"])
    assert (location.scheme, location.netloc, location.path) == ("https", "github.com", "/login/oauth/authorize")
    query = parse_qs(location.query)
    assert query["client_id"] == ["gh-client"]
    assert query["redirect_uri"] == ["https://agent-mail.example/mail/auth/github/callback"]
    return query["state"][0]


@pytest.mark.asyncio
async def test_github_sign_in(isolated_env, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []
    _fake_github(monkeypatch, "PeterRichards-LR", seen)
    async with await _client(monkeypatch, github=True) as client:
        page = await client.get("/mail/login")
        assert "Sign in with GitHub" in page.text and "Sign in with the server token instead" in page.text

        state = await _start_github(client)
        done = await client.get("/mail/auth/github/callback", params={"code": "good-code", "state": state})
        assert done.status_code == 303 and done.headers["location"] == "/mail/live"
        assert await _session_users() == ["peterrichards-lr"]  # who signed in
        assert (await client.get("/mail/live")).status_code == 200
        assert [r.url.host for r in seen] == ["github.com", "api.github.com"]

        # The one-time state can't be replayed.
        replay = await client.get("/mail/auth/github/callback", params={"code": "good-code", "state": state})
        assert replay.status_code == 400


@pytest.mark.asyncio
async def test_github_user_not_on_the_allowlist(isolated_env, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_github(monkeypatch, "stranger", [])
    async with await _client(monkeypatch, github=True) as client:
        state = await _start_github(client)
        refused = await client.get("/mail/auth/github/callback", params={"code": "good-code", "state": state})
        assert refused.status_code == 403 and "stranger is not allowed" in refused.text
        assert UI_SESSION_COOKIE not in client.cookies
        assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401


@pytest.mark.asyncio
async def test_github_callback_needs_the_state_from_this_browser(isolated_env, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []
    _fake_github(monkeypatch, "PeterRichards-LR", seen)
    async with await _client(monkeypatch, github=True) as client:
        # A callback link crafted elsewhere (no state cookie in this browser) signs nobody in.
        forged = await client.get("/mail/auth/github/callback", params={"code": "good-code", "state": "attacker"})
        assert forged.status_code == 400 and UI_SESSION_COOKIE not in client.cookies

        await _start_github(client)
        wrong = await client.get("/mail/auth/github/callback", params={"code": "good-code", "state": "other"})
        assert wrong.status_code == 400
        assert seen == []  # GitHub was never asked to exchange the code

        state = await _start_github(client)
        bad_code = await client.get("/mail/auth/github/callback", params={"code": "stolen", "state": state})
        assert bad_code.status_code == 502 and UI_SESSION_COOKIE not in client.cookies


@pytest.mark.asyncio
async def test_token_sign_in_can_be_turned_off(isolated_env, monkeypatch: pytest.MonkeyPatch) -> None:
    async with await _client(monkeypatch, github=True, token_login=False) as client:
        page = await client.get("/mail/login")
        assert "Sign in with GitHub" in page.text and 'name="token"' not in page.text
        refused = await _login(client)
        assert refused.status_code == 403 and UI_SESSION_COOKIE not in refused.cookies


@pytest.mark.asyncio
async def test_github_button_only_when_configured(client: AsyncClient) -> None:
    page = await client.get("/mail/login")
    assert "Sign in with GitHub" not in page.text and 'name="token"' in page.text
    assert (await client.get("/mail/auth/github")).status_code == 404
