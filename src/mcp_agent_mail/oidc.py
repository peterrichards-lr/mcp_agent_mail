"""Company sign-in for the web UI via OpenID Connect (Okta, Entra ID, Keycloak, Google Workspace, Auth0...).

Pattern from lfr-tunnel's sso.go. Authorization-code flow with PKCE (S256) and
a nonce; the ID token is verified against the provider's published keys
(JWKS) for signature, issuer, audience, expiry and nonce, and must carry a
verified email. Who may enter is decided in http.py from that email
(OIDC_ALLOWED_DOMAINS / OIDC_ALLOWED_EMAILS).

Configuration is the issuer URL plus a client id and secret; everything else
comes from the issuer's /.well-known/openid-configuration.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx
from authlib.jose import JsonWebKey, JsonWebToken
from authlib.jose.errors import JoseError

USER_AGENT = "mcp_agent_mail (+https://github.com/Dicklesworthstone/mcp_agent_mail)"
_DISCOVERY_CACHE_SECONDS = 3600.0
_discovery_cache: dict[str, tuple[dict[str, Any], float]] = {}
_jwks_cache: dict[str, tuple[dict[str, Any], float]] = {}


class OidcError(Exception):
    """Sign-in failed; the message is for logs and the audit trail, not necessarily for the person."""


def http_client() -> httpx.AsyncClient:
    """HTTP client for the identity provider (tests replace it with a fake provider)."""
    return httpx.AsyncClient(timeout=10.0, headers={"User-Agent": USER_AGENT})


@dataclass(frozen=True)
class AuthRequest:
    """What the browser is sent to the provider with, and what must be remembered for the callback."""

    authorize_url: str
    state: str
    nonce: str
    code_verifier: str


async def _get_json(client: httpx.AsyncClient, url: str, cache: dict[str, tuple[dict[str, Any], float]]) -> dict[str, Any]:
    cached = cache.get(url)
    if cached is not None and time.monotonic() - cached[1] < _DISCOVERY_CACHE_SECONDS:
        return cached[0]
    response = await client.get(url, headers={"Accept": "application/json"})
    response.raise_for_status()
    data = response.json()
    cache[url] = (data, time.monotonic())
    return data


async def discover(issuer: str) -> dict[str, Any]:
    async with http_client() as client:
        config = await _get_json(client, f"{issuer}/.well-known/openid-configuration", _discovery_cache)
    if config.get("issuer", "").rstrip("/") != issuer:
        raise OidcError(f"discovery document names issuer {config.get('issuer')!r}, expected {issuer!r}")
    return config


def _pkce_challenge(verifier: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")


async def begin(*, issuer: str, client_id: str, redirect_uri: str, scopes: str) -> AuthRequest:
    config = await discover(issuer)
    state, nonce, verifier = secrets.token_urlsafe(24), secrets.token_urlsafe(24), secrets.token_urlsafe(48)
    query = urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scopes,
        "state": state,
        "nonce": nonce,
        "code_challenge": _pkce_challenge(verifier),
        "code_challenge_method": "S256",
    })
    return AuthRequest(f"{config['authorization_endpoint']}?{query}", state, nonce, verifier)


async def finish(
    *, issuer: str, client_id: str, client_secret: str, redirect_uri: str, code: str, nonce: str, code_verifier: str
) -> dict[str, Any]:
    """Exchange the code and return the verified ID-token claims (with a verified, lower-cased `email`)."""
    config = await discover(issuer)
    form = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri, "code_verifier": code_verifier}
    methods = config.get("token_endpoint_auth_methods_supported") or ["client_secret_basic"]
    async with http_client() as client:
        if "client_secret_basic" in methods:
            token_response = await client.post(
                config["token_endpoint"], data=form, auth=(client_id, client_secret), headers={"Accept": "application/json"}
            )
        else:
            token_response = await client.post(
                config["token_endpoint"],
                data={**form, "client_id": client_id, "client_secret": client_secret},
                headers={"Accept": "application/json"},
            )
        if token_response.status_code != 200:
            raise OidcError(f"token endpoint returned {token_response.status_code}: {token_response.text[:200]}")
        id_token = token_response.json().get("id_token")
        if not id_token:
            raise OidcError("token response had no id_token")
        jwks = await _get_json(client, config["jwks_uri"], _jwks_cache)

    algorithms = config.get("id_token_signing_alg_values_supported") or ["RS256"]
    algorithms = [a for a in algorithms if a != "none"]  # never accept an unsigned token
    try:
        claims = JsonWebToken(algorithms).decode(
            id_token,
            JsonWebKey.import_key_set(jwks),
            claims_options={
                "iss": {"essential": True, "value": issuer},
                "aud": {"essential": True, "value": client_id},
                "exp": {"essential": True},
                "nonce": {"essential": True, "value": nonce},
            },
        )
        claims.validate(leeway=60)
    except (JoseError, ValueError) as exc:
        raise OidcError(f"ID token rejected: {exc}") from exc
    email = str(claims.get("email") or "").strip().lower()
    if not email or claims.get("email_verified") is False:
        raise OidcError("ID token has no verified email")
    return {**dict(claims), "email": email}
