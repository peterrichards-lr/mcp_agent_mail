"""Username + password + authenticator-app sign-in for the web UI (ui_auth.py)."""

from __future__ import annotations

import re
import time
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from typer.testing import CliRunner

from mcp_agent_mail import config as _config, ui_auth
from mcp_agent_mail.cli import app as cli_app
from mcp_agent_mail.http import UI_SESSION_COOKIE, build_http_app

TOKEN = "vps-secret-token"
PASSWORD = "correct horse battery staple"


def test_totp_matches_rfc_6238() -> None:
    # RFC 6238 appendix B, SHA-1 secret "12345678901234567890" at T=59 -> 94287082 (8 digits).
    secret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
    assert ui_auth.totp_code(secret, 59 // 30) == "287082"
    assert ui_auth.totp_code(secret, 1111111109 // 30) == "081804"


def test_password_hashing() -> None:
    stored = ui_auth.hash_password(PASSWORD)
    assert stored.startswith("scrypt$") and PASSWORD not in stored
    assert ui_auth.verify_password(PASSWORD, stored)
    assert not ui_auth.verify_password("wrong", stored)
    assert ui_auth.hash_password(PASSWORD) != stored  # salted


@pytest.fixture
async def client(isolated_env, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    monkeypatch.setenv("HTTP_BEARER_TOKEN", TOKEN)
    monkeypatch.setenv("HTTP_ALLOW_LOCALHOST_UNAUTHENTICATED", "false")
    _config.clear_settings_cache()
    app = build_http_app(_config.get_settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://agent-mail.example") as c:
        yield c


async def _password(client: AsyncClient, username: str = "alice", password: str = PASSWORD):
    return await client.post("/mail/auth/password", data={"username": username, "password": password, "next": "/mail/live"})


def _code(secret: str, offset_steps: int = 0) -> str:
    return ui_auth.totp_code(secret, int(time.time() // 30) + offset_steps)


async def _enroll(client: AsyncClient) -> tuple[str, list[str]]:
    """First sign-in: password, authenticator setup, recovery codes. Returns (secret, codes)."""
    first = await _password(client)
    assert first.status_code == 303 and first.headers["location"].startswith("/mail/auth/mfa/setup")
    setup = await client.get(first.headers["location"])
    assert setup.status_code == 200 and "otpauth://totp/Agent%20Mail%3Aalice" in setup.text
    shown = re.search(r'select-all">([A-Z2-7]{32})</code>', setup.text)
    assert shown is not None
    secret = shown.group(1)
    assert UI_SESSION_COOKIE not in client.cookies  # not signed in until the authenticator is confirmed

    wrong = await client.post("/mail/auth/mfa/setup", data={"code": "000000", "next": "/mail/live"})
    assert wrong.status_code == 401 and secret in wrong.text  # same secret offered again

    done = await client.post("/mail/auth/mfa/setup", data={"code": _code(secret), "next": "/mail/live"})
    assert done.status_code == 200 and "Shown only this once" in done.text
    codes = re.findall(r'select-all">([A-Z0-9]{5}-[A-Z0-9]{5})</li>', done.text)
    assert len(codes) == 10
    assert client.cookies[UI_SESSION_COOKIE].split(".")[1] == "local:alice"
    return secret, codes


@pytest.mark.asyncio
async def test_first_sign_in_enrolls_the_authenticator(client: AsyncClient) -> None:
    assert "Username" not in (await client.get("/mail/login")).text  # no accounts yet: no password form
    await ui_auth.create_user("alice", PASSWORD)
    assert 'name="username"' in (await client.get("/mail/login")).text

    await _enroll(client)
    assert (await client.get("/mail/live")).status_code == 200


@pytest.mark.asyncio
async def test_later_sign_ins_need_a_fresh_code(client: AsyncClient) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    secret, codes = await _enroll(client)
    client.cookies.clear()

    step_one = await _password(client)
    assert step_one.headers["location"].startswith("/mail/auth/mfa?")
    # The password alone opens nothing.
    assert (await client.get("/mail/api/events", params={"timeout": 0.1})).status_code == 401

    code = _code(secret, offset_steps=1)  # a code not used during enrolment
    assert (await client.post("/mail/auth/mfa", data={"code": code, "next": "/mail/live"})).status_code == 303
    assert (await client.get("/mail/live")).status_code == 200

    # The same code can't be replayed for another sign-in.
    client.cookies.clear()
    await _password(client)
    replay = await client.post("/mail/auth/mfa", data={"code": code, "next": "/mail/live"})
    assert replay.status_code == 401 and UI_SESSION_COOKIE not in client.cookies

    # A recovery code works, once.
    assert (await client.post("/mail/auth/mfa", data={"code": codes[0].lower(), "next": "/mail"})).status_code == 303
    client.cookies.clear()
    await _password(client)
    assert (await client.post("/mail/auth/mfa", data={"code": codes[0], "next": "/mail"})).status_code == 401


@pytest.mark.asyncio
async def test_wrong_passwords_and_unknown_users_look_the_same_then_lock(client: AsyncClient) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    unknown = await _password(client, username="mallory")
    wrong = await _password(client, password="not it")
    assert unknown.status_code == wrong.status_code == 401
    assert "Wrong username or password." in unknown.text and "Wrong username or password." in wrong.text

    for _ in range(4):
        await _password(client, password="not it")
    locked = await _password(client)  # even the right password, while locked
    assert locked.status_code == 401 and "Too many failed attempts" in locked.text


@pytest.mark.asyncio
async def test_mfa_steps_need_the_password_step_first(client: AsyncClient) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    assert (await client.get("/mail/auth/mfa")).headers["location"].startswith("/mail/login")
    assert (await client.post("/mail/auth/mfa", data={"code": "123456"})).headers["location"].startswith("/mail/login")
    assert (await client.get("/mail/auth/mfa/setup")).headers["location"].startswith("/mail/login")
    # A forged pending cookie is ignored.
    client.cookies.set("agent_mail_mfa", f"{int(time.time()) + 300}.1.enroll.bad", path="/mail/auth")
    assert (await client.get("/mail/auth/mfa/setup")).headers["location"].startswith("/mail/login")


@pytest.mark.asyncio
async def test_disabled_and_reset_accounts(client: AsyncClient) -> None:
    await ui_auth.create_user("alice", PASSWORD)
    await _enroll(client)
    client.cookies.clear()

    await ui_auth.reset_mfa("alice")  # lost phone
    assert (await _password(client)).headers["location"].startswith("/mail/auth/mfa/setup")

    await ui_auth.set_disabled("alice", True)
    client.cookies.clear()
    assert (await _password(client)).status_code == 401


def test_cli_manages_accounts(isolated_env) -> None:
    runner = CliRunner()
    added = runner.invoke(cli_app, ["users", "add", "bob", "--password-stdin"], input=PASSWORD + "\n")
    assert added.exit_code == 0, added.output
    assert runner.invoke(cli_app, ["users", "add", "bob", "--password-stdin"], input=PASSWORD + "\n").exit_code == 1
    short = runner.invoke(cli_app, ["users", "add", "carol", "--password-stdin"], input="short\n")
    assert short.exit_code == 1 and "at least 12" in short.output
    listed = runner.invoke(cli_app, ["users", "list"])
    assert listed.exit_code == 0 and "bob" in listed.output and "not set up yet" in listed.output
    assert runner.invoke(cli_app, ["users", "disable", "bob"]).exit_code == 0
    assert "disabled" in runner.invoke(cli_app, ["users", "list"]).output
