"""Browser login for the /mail UI on a server that requires a bearer token (e.g. on a VPS)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from mcp_agent_mail import config as _config
from mcp_agent_mail.http import UI_SESSION_COOKIE, build_http_app

TOKEN = "vps-secret-token"


async def _client(monkeypatch: pytest.MonkeyPatch, *, jwt: bool = False) -> AsyncClient:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    if jwt:
        monkeypatch.setenv("HTTP_JWT_ENABLED", "true")
        monkeypatch.setenv("HTTP_JWT_SECRET", "jwt-secret")
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
async def test_tampered_or_expired_cookie_is_rejected(client: AsyncClient) -> None:
    await _login(client)
    expires, _, signature = client.cookies[UI_SESSION_COOKIE].partition(".")
    client.cookies.clear()

    client.cookies.set(UI_SESSION_COOKIE, f"{int(expires) + 1}.{signature}", path="/mail")
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401

    client.cookies.clear()
    client.cookies.set(UI_SESSION_COOKIE, f"1.{signature}", path="/mail")
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401


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
