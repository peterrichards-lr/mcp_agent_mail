"""Server-side web sessions (revocable, sliding, re-checked) and the security audit log."""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from typer.testing import CliRunner

from mcp_agent_mail import audit, config as _config, ui_auth
from mcp_agent_mail.cli import app as cli_app
from mcp_agent_mail.config import get_settings
from mcp_agent_mail.db import ensure_schema, get_database_path, get_session
from mcp_agent_mail.http import UI_SESSION_COOKIE, build_http_app
from mcp_agent_mail.share import apply_project_scope, scrub_snapshot

TOKEN = "vps-secret-token"
PASSWORD = "correct horse battery staple"


@pytest.fixture
async def client(isolated_env, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    _config.clear_settings_cache()
    ui_auth._session_cache.clear()
    ui_auth._account_cache.clear()
    await ensure_schema()
    async with AsyncClient(transport=ASGITransport(app=build_http_app(get_settings())), base_url="https://am.example") as c:
        yield c


async def _token_sign_in(client: AsyncClient) -> None:
    assert (await client.post("/mail/login", data={"token": TOKEN, "next": "/mail"})).status_code == 303


async def _authorized(client: AsyncClient) -> bool:
    return (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 200


async def _password_sign_in(client: AsyncClient) -> None:
    """Sign alice in with password + a code (enrolling on the first call)."""
    import re
    import time

    first = await client.post("/mail/auth/password", data={"username": "alice", "password": PASSWORD, "next": "/mail"})
    setup = await client.get(first.headers["location"])
    shown = re.search(r'select-all">([A-Z2-7]{32})</code>', setup.text)
    assert shown is not None
    code = ui_auth.totp_code(shown.group(1), int(time.time() // 30))
    assert (await client.post("/mail/auth/mfa/setup", data={"code": code, "next": "/mail"})).status_code == 200


async def _actions() -> list[tuple[str, str]]:
    return [(e.actor, e.action) for e in reversed(await audit.recent(limit=1000))]


@pytest.mark.asyncio
async def test_disabling_an_account_ends_its_open_session_at_once(client: AsyncClient) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    await _password_sign_in(client)
    assert await _authorized(client)

    await ui_auth.set_disabled("alice", True)
    assert not await _authorized(client)  # same cookie, no new sign-in attempt


@pytest.mark.asyncio
async def test_removal_from_the_github_allowlist_ends_sessions(client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_OAUTH_CLIENT_ID", "id")
    monkeypatch.setenv("GITHUB_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("GITHUB_ALLOWED_USERS", "colleague")
    _config.clear_settings_cache()
    token = await ui_auth.create_session("colleague", "github")
    client.cookies.set(UI_SESSION_COOKIE, token, path="/mail")
    assert await _authorized(client)

    monkeypatch.setenv("GITHUB_ALLOWED_USERS", "someone-else")
    _config.clear_settings_cache()
    assert not await _authorized(client)


@pytest.mark.asyncio
async def test_background_polling_does_not_keep_a_session_alive(client: AsyncClient) -> None:
    await _token_sign_in(client)
    two_hours_ago = ui_auth._now() - timedelta(hours=2)

    async def last_seen() -> object:
        async with get_session() as session:
            return (await session.execute(text("SELECT last_seen_ts FROM ui_sessions"))).scalar_one()

    async def age_it() -> None:
        async with get_session() as session:
            await session.execute(text("UPDATE ui_sessions SET last_seen_ts = :t"), {"t": two_hours_ago})
            await session.commit()
        ui_auth._session_cache.clear()

    await age_it()
    before = await last_seen()
    assert await _authorized(client)  # /mail/api/ traffic (the Live page's poll and stream)
    assert await last_seen() == before
    assert (await client.get("/mail/live")).status_code == 200  # a person opening a page
    assert await last_seen() != before


@pytest.mark.asyncio
async def test_account_page_lists_and_revokes_sessions(client: AsyncClient) -> None:
    await _token_sign_in(client)
    other = await ui_auth.create_session(TOKEN_USER := "_token", "token", ip="203.0.113.9", user_agent="other laptop")
    page = await client.get("/mail/account/tokens")
    assert page.status_code == 200 and "this browser" in page.text and "203.0.113.9" in page.text

    [other_row] = [s for s in await ui_auth.list_sessions(TOKEN_USER) if s.ip == "203.0.113.9"]
    assert (await client.post(f"/mail/account/sessions/{other_row.id}/revoke")).status_code == 303
    assert await ui_auth.lookup_session(other, activity=False) is None
    assert await _authorized(client)  # this browser is unaffected

    everywhere = await client.post("/mail/account/sessions/revoke-all")
    assert everywhere.headers["location"] == "/mail/login"
    assert not await _authorized(client) and await ui_auth.list_sessions(TOKEN_USER) == []


@pytest.mark.asyncio
async def test_audit_records_sign_ins_failures_and_lockouts(client: AsyncClient) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    await client.post("/mail/login", data={"token": "wrong", "next": "/mail"})
    for _ in range(5):
        await client.post("/mail/auth/password", data={"username": "alice", "password": "nope", "next": "/mail"})
    await _token_sign_in(client)
    await client.post("/mail/logout")

    actions = await _actions()
    assert ("_token", "auth.signin_failed") in actions
    assert actions.count(("local:alice", "auth.signin_failed")) == 5
    assert ("local:alice", "auth.locked") in actions
    assert ("_token", "auth.signin") in actions and ("_token", "auth.signout") in actions
    [entry] = [e for e in await audit.recent(actor="_token", action_prefix="auth.signin") if e.action == "auth.signin"]
    assert entry.details == "method=token" and entry.ip  # where it came from


@pytest.mark.asyncio
async def test_audit_records_tokens_and_destructive_actions(client: AsyncClient) -> None:
    await _token_sign_in(client)
    state = "s" * 24
    await client.post("/mail/auth/cli", data={"state": state, "name": "laptop"})
    [token] = await ui_auth.list_api_tokens("_token")
    await client.post(f"/mail/account/tokens/{token.id}/revoke")
    async with get_session() as session:
        await session.execute(text("INSERT INTO projects (id, slug, human_key, created_at) VALUES (7, 'p', 'p', '2026-01-01')"))
        await session.commit()
    assert (await client.post("/mail/api/archive-project", json={"project_id": 7})).status_code == 200

    actions = await _actions()
    for expected in (("_token", "token.created"), ("_token", "token.revoked"), ("_token", "project.archived")):
        assert expected in actions
    page = await client.get("/mail/audit", params={"action": "token."})
    assert page.status_code == 200 and "token.created" in page.text and "project.archived" not in page.text


def test_cli_account_changes_are_audited(isolated_env) -> None:
    runner = CliRunner()
    assert runner.invoke(cli_app, ["users", "add", "bob", "--password-stdin"], input=PASSWORD + "\n").exit_code == 0
    assert runner.invoke(cli_app, ["users", "disable", "bob"]).exit_code == 0
    import asyncio

    entries = asyncio.run(audit.recent())
    assert [(e.actor, e.action, e.target_id) for e in reversed(entries)] == [
        ("cli", "user.created", "local:bob"),
        ("cli", "user.disabled", "local:bob"),
    ]


@pytest.mark.asyncio
async def test_audit_retention(isolated_env) -> None:
    await audit.record("auth.signin", actor="old")
    await audit.record("auth.signin", actor="new")
    async with get_session() as session:
        await session.execute(text("UPDATE audit_log SET ts = :t WHERE actor = 'old'"), {"t": ui_auth._now() - timedelta(days=400)})
        await session.commit()
    assert await audit.prune(365) == 1
    assert [e.actor for e in await audit.recent()] == ["new"]
    assert await audit.prune(0) == 0  # 0 keeps everything


@pytest.mark.asyncio
async def test_share_exports_carry_no_credentials_or_audit(client: AsyncClient, tmp_path: Path) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    await _token_sign_in(client)
    await ui_auth.create_api_token("_token", "laptop")
    async with get_session() as session:
        await session.execute(text("INSERT INTO projects (id, slug, human_key, created_at) VALUES (1, 'a', 'a', '2026-01-01'), (2, 'b', 'b', '2026-01-01')"))
        await session.execute(text(
            "INSERT INTO file_attachments (id, project_id, uploader, filename, media_type, size, sha256, created_ts) "
            "VALUES ('att_x', 2, 'u', 'f', 'text/plain', 1, 'h', '2026-01-01')"
        ))
        await session.commit()
    db_path = get_database_path(get_settings())
    assert db_path is not None
    snapshot = tmp_path / "snapshot.sqlite3"
    with sqlite3.connect(db_path) as source, sqlite3.connect(snapshot) as dest:
        source.backup(dest)

    apply_project_scope(snapshot, ["a"])  # project b goes, with the attachment row that references it
    scrub_snapshot(snapshot)
    with sqlite3.connect(snapshot) as conn:
        for table in ("ui_users", "ui_sessions", "api_tokens", "audit_log", "agent_presence", "events"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0, table
        assert conn.execute("SELECT COUNT(*) FROM file_attachments").fetchone()[0] == 0
