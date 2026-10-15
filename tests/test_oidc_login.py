"""Company sign-in via OpenID Connect, against a fake identity provider that signs real ID tokens."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from authlib.jose import JsonWebKey, jwt
from httpx import ASGITransport, AsyncClient

from mcp_agent_mail import audit, config as _config, oidc, ui_auth
from mcp_agent_mail.db import ensure_schema
from mcp_agent_mail.http import UI_SESSION_COOKIE, build_http_app

TOKEN = "vps-secret-token"
ISSUER = "https://idp.example.com"
CLIENT_ID = "agent-mail"
PRIVATE_KEY = JsonWebKey.generate_key("RSA", 2048, is_private=True).as_dict(is_private=True) | {"kid": "k1"}
OTHER_KEY = JsonWebKey.generate_key("RSA", 2048, is_private=True).as_dict(is_private=True) | {"kid": "k1"}


class FakeIdp:
    """Discovery, JWKS and a token endpoint; `claims` tweaks what the next ID token says."""

    def __init__(self) -> None:
        self.claims: dict[str, Any] = {}
        self.signing_key: dict[str, Any] = PRIVATE_KEY
        self.last_token_request: httpx.Request | None = None
        self.nonce = ""

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/.well-known/openid-configuration":
            return httpx.Response(200, json={
                "issuer": ISSUER, "authorization_endpoint": f"{ISSUER}/authorize", "token_endpoint": f"{ISSUER}/token",
                "jwks_uri": f"{ISSUER}/jwks", "id_token_signing_alg_values_supported": ["RS256"],
                "token_endpoint_auth_methods_supported": ["client_secret_basic"],
            })
        if path == "/jwks":
            public = JsonWebKey.import_key(PRIVATE_KEY).as_dict(is_private=False) | {"kid": "k1", "alg": "RS256", "use": "sig"}
            return httpx.Response(200, json={"keys": [public]})
        if path == "/token":
            self.last_token_request = request
            if parse_qs(request.content.decode()).get("code") != ["good-code"]:
                return httpx.Response(400, json={"error": "invalid_grant"})
            now = int(time.time())
            claims = {"iss": ISSUER, "aud": CLIENT_ID, "sub": "u1", "iat": now, "exp": now + 300, "nonce": self.nonce,
                      "email": "Pat@Liferay.com", "email_verified": True} | self.claims
            token = jwt.encode({"alg": "RS256", "kid": "k1"}, claims, self.signing_key).decode()
            return httpx.Response(200, json={"access_token": "at", "id_token": token, "token_type": "Bearer"})
        return httpx.Response(404)


@pytest.fixture
async def setup(isolated_env, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Callable[..., Any]]:
    idp = FakeIdp()
    monkeypatch.setattr(oidc, "http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(idp.handler)))
    oidc._discovery_cache.clear()
    oidc._jwks_cache.clear()
    clients: list[AsyncClient] = []

    async def make(**env: str) -> tuple[AsyncClient, FakeIdp]:
        base = {"HTTP_BEARER_TOKEN": TOKEN, "HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED": "false", "OIDC_ISSUER": ISSUER,
                "OIDC_CLIENT_ID": CLIENT_ID, "OIDC_CLIENT_SECRET": "s3cret", "OIDC_ALLOWED_DOMAINS": "liferay.com",
                "OIDC_DISPLAY_NAME": "Liferay SSO"}
        for key, value in (base | env).items():
            monkeypatch.setenv(key, value)
        _config.clear_settings_cache()
        await ensure_schema()
        client = AsyncClient(transport=ASGITransport(app=build_http_app(_config.get_settings())), base_url="https://am.example")
        clients.append(client)
        return client, idp

    yield make
    for client in clients:
        await client.aclose()


async def _sign_in(client: AsyncClient, idp: FakeIdp, *, code: str = "good-code", tamper_state: bool = False) -> httpx.Response:
    start = await client.get("/mail/auth/oidc", params={"next": "/mail/live"})
    assert start.status_code == 303
    query = parse_qs(urlparse(start.headers["location"]).query)
    assert query["code_challenge_method"] == ["S256"] and query["redirect_uri"] == ["https://am.example/mail/auth/oidc/callback"]
    idp.nonce = query["nonce"][0]
    state = "forged" if tamper_state else query["state"][0]
    return await client.get("/mail/auth/oidc/callback", params={"code": code, "state": state})


async def _authorized(client: AsyncClient) -> bool:
    return (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 200


@pytest.mark.asyncio
async def test_company_sign_in(setup) -> None:
    client, idp = await setup()
    page = await client.get("/mail/login")
    assert "Sign in with Liferay SSO" in page.text
    done = await _sign_in(client, idp)
    assert done.status_code == 303 and done.headers["location"] == "/mail/live"
    assert await _authorized(client)
    assert [s.user for s in await ui_auth.list_sessions("sso:pat@liferay.com")] == ["sso:pat@liferay.com"]
    # PKCE: the verifier went to the token endpoint, and the client authenticated with HTTP Basic.
    assert idp.last_token_request is not None
    assert "code_verifier=" in idp.last_token_request.content.decode()
    assert idp.last_token_request.headers["Authorization"].startswith("Basic ")


@pytest.mark.parametrize(
    ("claims", "key"),
    [
        ({"aud": "someone-else"}, None),  # token minted for another app
        ({"iss": "https://evil.example"}, None),
        ({"nonce": "replayed"}, None),
        ({"exp": int(time.time()) - 600}, None),
        ({"email_verified": False}, None),
        ({}, OTHER_KEY),  # signed by a key the provider never published
    ],
)
@pytest.mark.asyncio
async def test_bad_id_tokens_are_rejected(setup, claims: dict[str, Any], key: dict[str, Any] | None) -> None:
    client, idp = await setup()
    idp.claims = claims
    if key is not None:
        idp.signing_key = key
    rejected = await _sign_in(client, idp)
    assert rejected.status_code == 502 and UI_SESSION_COOKIE not in client.cookies
    assert ("sso:?", "auth.signin_failed") in [(e.actor, e.action) for e in await audit.recent()]


@pytest.mark.asyncio
async def test_forged_state_and_outside_domains(setup) -> None:
    client, idp = await setup()
    assert (await _sign_in(client, idp, tamper_state=True)).status_code == 400
    idp.claims = {"email": "someone@gmail.com"}
    refused = await _sign_in(client, idp)
    assert refused.status_code == 403 and "someone@gmail.com is not allowed" in refused.text

    client2, idp2 = await setup(OIDC_ALLOWED_DOMAINS="", OIDC_ALLOWED_EMAILS="pat@liferay.com")
    idp2.claims = {}
    assert (await _sign_in(client2, idp2)).status_code == 303


@pytest.mark.asyncio
async def test_removing_a_domain_ends_sessions(setup) -> None:
    client, idp = await setup()
    await _sign_in(client, idp)
    assert await _authorized(client)
    await setup(OIDC_ALLOWED_DOMAINS="example.org")  # operator narrows access
    assert not await _authorized(client)


@pytest.mark.asyncio
async def test_sso_only_mode_closes_every_other_door(setup) -> None:
    client, _idp = await setup()
    await ui_auth.create_user("alice", "correct horse battery staple")
    assert (await client.post("/mail/login", data={"token": TOKEN, "next": "/mail"})).status_code == 303  # works today
    assert await _authorized(client)

    sso_client, sso_idp = await setup(HTTP_UI_SSO_ONLY="true")
    assert not await _authorized(client)  # the token session ends at once
    page = await sso_client.get("/mail/login")
    assert "Sign in with Liferay SSO" in page.text and 'name="token"' not in page.text and 'name="username"' not in page.text
    assert (await sso_client.post("/mail/login", data={"token": TOKEN})).status_code == 403
    password = await sso_client.post("/mail/auth/password", data={"username": "alice", "password": "correct horse battery staple"})
    assert password.status_code == 403
    assert (await _sign_in(sso_client, sso_idp)).status_code == 303
    assert await _authorized(sso_client)
