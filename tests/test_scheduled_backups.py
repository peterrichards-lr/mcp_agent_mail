"""Backups: lossless archives keep server state and attachments, and the server takes them on a schedule."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fastmcp import Client
from sqlalchemy import text
from typer.testing import CliRunner

from mcp_agent_mail import audit, cli as cli_module, config as _config, ui_auth
from mcp_agent_mail.app import _blob_path, _store_file_attachment, build_mcp_server
from mcp_agent_mail.cli import app as cli_app, run_scheduled_backup
from mcp_agent_mail.config import get_settings
from mcp_agent_mail.db import get_session, reset_database_state
from mcp_agent_mail.models import Project

runner = CliRunner()
PASSWORD = "correct horse battery staple"


@pytest.fixture
def backup_dir(isolated_env, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    target = tmp_path / "backups"
    monkeypatch.setenv("BACKUP_DIR", str(target))
    _config.clear_settings_cache()
    return target


async def _chunks(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def _seed() -> str:
    """Mail, an account, a token, an audit entry and an uploaded attachment; returns the blob's sha256."""
    async with Client(build_mcp_server()) as client:
        await client.call_tool("ensure_project", {"human_key": "/backend"})
        await client.call_tool("register_agent", {"project_key": "/backend", "program": "x", "model": "y", "name": "BlueLake"})
        await client.call_tool(
            "send_message",
            {"project_key": "/backend", "sender_name": "BlueLake", "to": ["BlueLake"], "subject": "keep me", "body_md": "b"},
        )
    await ui_auth.create_user("alice", PASSWORD)
    await ui_auth.create_api_token("alice", "laptop")
    await audit.record("auth.signin", actor="local:alice")
    async with get_session() as session:
        project = (await session.execute(text("SELECT id, slug, human_key FROM projects"))).one()
    meta = await _store_file_attachment(
        Project(id=project[0], slug=project[1], human_key=project[2]), "BlueLake", "report.txt",
        _chunks(b"verification: PASS\n"), get_settings(),
    )
    return str(meta["sha256"])


async def _counts() -> dict[str, int]:
    async with get_session() as session:
        return {
            table: (await session.execute(text(f"SELECT COUNT(*) FROM {table}"))).scalar_one()
            for table in ("messages", "ui_users", "api_tokens", "audit_log", "file_attachments")
        }


@pytest.mark.asyncio
async def test_lossless_archive_round_trip_keeps_accounts_tokens_audit_and_attachments(backup_dir: Path) -> None:
    sha = await _seed()
    before = await _counts()
    assert all(before.values()), before

    saved = runner.invoke(cli_app, ["archive", "save", "--label", "before-change"])
    assert saved.exit_code == 0, saved.output
    [archive] = list(backup_dir.glob("*.zip"))

    # Disaster: the account is gone and the attachment bytes are lost.
    async with get_session() as session:
        await session.execute(text("DELETE FROM api_tokens"))
        await session.execute(text("DELETE FROM ui_users"))
        await session.commit()
    _blob_path(get_settings(), sha).unlink()
    reset_database_state()  # the restore replaces the database file underneath any open engine

    restored = runner.invoke(cli_app, ["archive", "restore", str(archive), "--force"])
    assert restored.exit_code == 0, restored.output
    reset_database_state()
    assert await _counts() == before
    assert _blob_path(get_settings(), sha).read_bytes() == b"verification: PASS\n"
    assert await ui_auth.get_user("alice") is not None


@pytest.mark.asyncio
async def test_share_presets_still_drop_server_state(backup_dir: Path) -> None:
    await _seed()
    saved = runner.invoke(cli_app, ["archive", "save", "--scrub-preset", "standard"])
    assert saved.exit_code == 0, saved.output
    [archive] = list(backup_dir.glob("*standard*.zip"))
    import sqlite3
    import zipfile

    with zipfile.ZipFile(archive) as zf:
        snapshot = backup_dir / "snap.sqlite3"
        snapshot.write_bytes(zf.read(cli_module.ARCHIVE_SNAPSHOT_RELATIVE.as_posix()))
    with sqlite3.connect(snapshot) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ui_users").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_scheduled_backups_run_when_due_and_keep_the_newest(backup_dir: Path) -> None:
    await _seed()
    assert run_scheduled_backup(interval_hours=0, keep=3) is None  # off

    first = run_scheduled_backup(interval_hours=24, keep=3)
    assert first is not None and first.parent == backup_dir and "archive-scheduled" in first.name
    assert run_scheduled_backup(interval_hours=24, keep=3) is None  # not due yet

    manual = runner.invoke(cli_app, ["archive", "save", "--label", "before-upgrade"])
    assert manual.exit_code == 0
    made = [first]
    for _ in range(4):
        for archive in backup_dir.glob("*scheduled*.zip"):  # pretend the last one is a day old
            os.utime(archive, (time.time() - 25 * 3600, time.time() - 25 * 3600))
        await asyncio.sleep(1.1)  # distinct timestamps in the names
        newest = run_scheduled_backup(interval_hours=24, keep=3)
        assert newest is not None
        made.append(newest)

    scheduled = sorted(p.name for p in backup_dir.glob("*scheduled*.zip"))
    assert scheduled == sorted(p.name for p in made[-3:])  # the newest three
    assert len(list(backup_dir.glob("*before-upgrade*.zip"))) == 1  # manual archives are never pruned


def test_one_worker_backs_up_at_a_time(backup_dir: Path) -> None:
    import fcntl

    backup_dir.mkdir(parents=True)
    with (backup_dir / ".scheduled-backup.lock").open("a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)  # another worker is mid-backup
        assert run_scheduled_backup(interval_hours=24, keep=3) is None
