"""Cross-machine file attachments: upload/download with scripts/hooks/agent_mail_files.py against a real server."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

import pytest
from fastmcp.exceptions import ToolError

from mcp_agent_mail.app import _repo_project_slug
from tests.live_server import PROJECT, TOKEN, Mail

HELPER = Path(__file__).resolve().parents[1] / "scripts" / "hooks" / "agent_mail_files.py"


def _config(server_url: str, tmp_path: Path, agent: str) -> Path:
    path = tmp_path / f"{agent}.json"
    path.write_text(json.dumps({
        "AGENT_MAIL_SERVER_URL": server_url,
        "AGENT_MAIL_TOKEN": TOKEN,
        "AGENT_MAIL_PROJECT": PROJECT,
        "AGENT_MAIL_AGENT": agent,
    }))
    return path


def _helper(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(HELPER), *map(str, args)],
        env={"PATH": os.environ["PATH"]}, capture_output=True, text=True, timeout=60,
    )


def _upload(server_url: str, tmp_path: Path, file: Path, agent: str = "BlueLake") -> dict:
    done = _helper("upload", file, "--config", _config(server_url, tmp_path, agent))
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


@pytest.mark.asyncio
async def test_report_travels_byte_for_byte(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    report = tmp_path / "worker" / "verify-windows-pc-windows-11-powershell-7-docker-desktop-pass.txt"
    report.parent.mkdir()
    report.write_bytes("✅ 42 checks passed\r\nLDM 2.27.0\n".encode() + bytes(range(256)))

    meta = _upload(server_url, tmp_path, report)
    assert meta["name"] == report.name and meta["size"] == report.stat().st_size and meta["id"].startswith("att_")

    result = await mcp.client.call_tool(
        "send_message",
        {"project_key": PROJECT, "sender_name": "BlueLake", "sender_token": mcp.tokens["BlueLake"],
         "to": ["GreenCastle"], "subject": "Report", "body_md": "Attached.", "attachment_ids": [meta["id"]],
         "thread_id": "verify-v2.27.0"},
    )
    sent = result.data["deliveries"][0]["payload"]["attachments"]
    assert sent == [{"type": "file", **{k: meta[k] for k in ("id", "name", "size", "sha256", "media_type")}}]

    inbox = await mcp.client.call_tool(
        "fetch_inbox", {"project_key": PROJECT, "agent_name": "GreenCastle", "registration_token": mcp.tokens["GreenCastle"]}
    )
    assert inbox.structured_content is not None
    received = inbox.structured_content["result"]  # plain dicts (.data wraps list items in models)
    [attachment] = next(m for m in received if m["subject"] == "Report")["attachments"]

    results = tmp_path / "coordinator" / "references" / "verification-results"
    results.mkdir(parents=True)
    coordinator = _config(server_url, tmp_path, "GreenCastle")
    done = _helper("download", attachment["id"], "-o", results, "--config", coordinator)
    assert done.returncode == 0, done.stderr
    assert (results / report.name).read_bytes() == report.read_bytes()
    assert f"sha256 {meta['sha256']} (verified)" in done.stdout

    # Downloaded reports are records: never silently overwritten.
    again = _helper("download", attachment["id"], "-o", results, "--config", coordinator)
    assert again.returncode == 1 and "already exists" in again.stderr
    assert _helper("download", attachment["id"], "-o", results, "--force", "--config", coordinator).returncode == 0


@pytest.mark.asyncio
async def test_binary_files_and_replies(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    blob = tmp_path / "bundle.tar.gz"
    blob.write_bytes(os.urandom(3 * 1024 * 1024 + 17))
    meta = _upload(server_url, tmp_path, blob, agent="GreenCastle")

    first = await mcp.send("BlueLake", ["GreenCastle"], "Please send the bundle")
    reply = await mcp.client.call_tool(
        "reply_message",
        {"project_key": PROJECT, "message_id": first, "sender_name": "GreenCastle",
         "sender_token": mcp.tokens["GreenCastle"], "body_md": "Here it is.", "attachment_ids": [meta["id"]]},
    )
    assert reply.data["deliveries"][0]["payload"]["attachments"][0]["id"] == meta["id"]

    out = tmp_path / "out.tar.gz"
    done = _helper("download", meta["id"], "-o", out, "--config", _config(server_url, tmp_path, "BlueLake"))
    assert done.returncode == 0, done.stderr
    assert out.read_bytes() == blob.read_bytes()


@pytest.mark.asyncio
async def test_unknown_attachment_id_fails_before_delivery(mcp: Mail, server_url: str) -> None:
    with pytest.raises(ToolError, match="att_nope"):
        await mcp.client.call_tool(
            "send_message",
            {"project_key": PROJECT, "sender_name": "BlueLake", "sender_token": mcp.tokens["BlueLake"],
             "to": ["GreenCastle"], "subject": "Ghost", "body_md": "x", "attachment_ids": ["att_nope"]},
        )
    inbox = await mcp.client.call_tool(
        "fetch_inbox", {"project_key": PROJECT, "agent_name": "GreenCastle", "registration_token": mcp.tokens["GreenCastle"]}
    )
    assert inbox.structured_content is not None
    assert "Ghost" not in json.dumps(inbox.structured_content["result"])


@pytest.mark.asyncio
async def test_tampered_blob_is_rejected(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    report = tmp_path / "report.txt"
    report.write_text("all green\n")
    meta = _upload(server_url, tmp_path, report)
    # isolated_env sets STORAGE_ROOT=<tmp>/storage, so blobs live in <tmp>/storage-blobs.
    blob = tmp_path / "storage-blobs" / meta["sha256"][:2] / meta["sha256"]
    blob.write_text("all red\n")

    out = tmp_path / "got"
    done = _helper("download", meta["id"], "-o", out, "--config", _config(server_url, tmp_path, "GreenCastle"))
    assert done.returncode == 1 and "checksum mismatch" in done.stderr
    assert not out.exists() and not list(tmp_path.glob(".got*"))


@pytest.fixture
def small_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATTACHMENT_MAX_BYTES", "1000")


@pytest.mark.asyncio
async def test_size_limit(small_limit: None, mcp: Mail, server_url: str, tmp_path: Path) -> None:
    big = tmp_path / "big.log"
    big.write_bytes(b"x" * 1001)
    done = _helper("upload", big, "--config", _config(server_url, tmp_path, "BlueLake"))
    assert done.returncode == 1 and "413" in done.stderr
    assert not list((tmp_path / "storage-blobs").rglob("*")) or all(
        p.is_dir() for p in (tmp_path / "storage-blobs").rglob("*")
    )


@pytest.mark.asyncio
async def test_message_page_links_the_download(mcp: Mail, server_url: str, tmp_path: Path) -> None:
    report = tmp_path / "report.txt"
    report.write_text("ok\n")
    meta = _upload(server_url, tmp_path, report)
    result = await mcp.client.call_tool(
        "send_message",
        {"project_key": PROJECT, "sender_name": "BlueLake", "sender_token": mcp.tokens["BlueLake"],
         "to": ["GreenCastle"], "subject": "Linked", "body_md": "See file.", "attachment_ids": [meta["id"]]},
    )
    mid = result.data["deliveries"][0]["payload"]["id"]
    slug = _repo_project_slug("github.com/owner/wake")
    request = urllib.request.Request(
        f"{server_url}/mail/{slug}/message/{mid}", headers={"Authorization": f"Bearer {TOKEN}"}
    )

    def fetch() -> str:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read().decode()

    page = await asyncio.to_thread(fetch)
    assert f'href="/mail/api/attachments/{meta["id"]}"' in page and "report.txt" in page
