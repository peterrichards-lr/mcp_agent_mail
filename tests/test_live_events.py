"""Live event stream: the `events` log and the `/mail/api/events` SSE endpoint."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError
from httpx import ASGITransport, AsyncClient
from sqlmodel import col, select

from mcp_agent_mail.app import build_mcp_server
from mcp_agent_mail.config import get_settings
from mcp_agent_mail.db import get_database_path, get_session
from mcp_agent_mail.http import build_http_app
from mcp_agent_mail.models import Event
from mcp_agent_mail.share import apply_project_scope, scrub_snapshot


def _data(result: Any) -> Any:
    if getattr(result, "data", None) is not None:
        return result.data
    return json.loads(result.content[0].text)


def _parse_sse(body: str) -> list[dict[str, Any]]:
    """Return the data payloads of the SSE frames in ``body``, checking id/event lines match."""
    events: list[dict[str, Any]] = []
    for frame in body.split("\n\n"):
        fields: dict[str, str] = {}
        for line in frame.splitlines():
            if line.startswith(":") or ": " not in line:
                continue
            key, value = line.split(": ", 1)
            fields[key] = value
        if "data" not in fields:
            continue
        data = json.loads(fields["data"])
        assert fields["id"] == str(data["id"])
        assert fields["event"] == data["type"]
        events.append(data)
    return events


async def _events() -> list[Event]:
    async with get_session() as session:
        return list((await session.execute(select(Event).order_by(col(Event.id)))).scalars().all())


async def _send(client: Client, project: str, sender: str, to: list[str], subject: str, **extra: Any) -> dict[str, Any]:
    result = await client.call_tool(
        "send_message",
        {"project_key": project, "sender_name": sender, "to": to, "subject": subject, "body_md": "body", **extra},
    )
    return _data(result)["deliveries"][0]["payload"]


@pytest.fixture
async def seeded(isolated_env) -> AsyncIterator[Client]:
    """Two projects: alpha (BlueLake, GreenCastle) and beta (RedStone)."""
    async with Client(build_mcp_server()) as client:
        for key in ("/alpha", "/beta"):
            await client.call_tool("ensure_project", {"human_key": key})
        for project, name in (("/alpha", "BlueLake"), ("/alpha", "GreenCastle"), ("/beta", "RedStone")):
            await client.call_tool(
                "register_agent", {"project_key": project, "program": "claude-code", "model": "opus", "name": name}
            )
        yield client


@pytest.fixture
async def http_client(seeded: Client) -> AsyncIterator[AsyncClient]:
    app = build_http_app(get_settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_mail_lifecycle_is_recorded(seeded: Client) -> None:
    sent = await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], "Plan", thread_id="br-1", ack_required=True)
    mid = sent["id"]
    await seeded.call_tool("acknowledge_message", {"project_key": "/alpha", "agent_name": "GreenCastle", "message_id": mid})
    # Repeating read/ack changes nothing, so it must not emit again.
    await seeded.call_tool("mark_message_read", {"project_key": "/alpha", "agent_name": "GreenCastle", "message_id": mid})
    await seeded.call_tool("acknowledge_message", {"project_key": "/alpha", "agent_name": "GreenCastle", "message_id": mid})

    events = await _events()
    registered = [e for e in events if e.type == "agent.registered"]
    assert [(e.actor, e.payload["created"]) for e in registered] == [
        ("BlueLake", True),
        ("GreenCastle", True),
        ("RedStone", True),
    ]
    mail = [(e.type, e.actor) for e in events if e.type.startswith("message.")]
    assert mail == [("message.sent", "BlueLake"), ("message.read", "GreenCastle"), ("message.acked", "GreenCastle")]
    sent_event = next(e for e in events if e.type == "message.sent")
    assert sent_event.payload == {
        "message_id": mid,
        "thread_id": "br-1",
        "subject": "Plan",
        "importance": "normal",
        "ack_required": True,
        "project": "alpha",
        "to": ["GreenCastle"],
        "cc": [],
        "bcc": [],
    }
    acked = next(e for e in events if e.type == "message.acked")
    assert acked.payload == {"message_id": mid, "thread_id": "br-1", "subject": "Plan", "sender": "BlueLake"}


@pytest.mark.asyncio
async def test_reregistration_is_not_reported_as_new(seeded: Client) -> None:
    await seeded.call_tool(
        "register_agent", {"project_key": "/alpha", "program": "gemini-cli", "model": "gemini", "name": "BlueLake"}
    )
    last = (await _events())[-1]
    assert (last.type, last.actor, last.payload["created"], last.payload["program"]) == (
        "agent.registered",
        "BlueLake",
        False,
        "gemini-cli",
    )


@pytest.mark.asyncio
async def test_failed_archive_write_emits_no_sent_event(seeded: Client, monkeypatch: pytest.MonkeyPatch) -> None:
    import mcp_agent_mail.app as app_module

    async def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("simulated archive write failure")

    monkeypatch.setattr(app_module, "write_message_bundle", _boom)
    with pytest.raises(ToolError):
        await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], "Lost")
    assert not [e for e in await _events() if e.type == "message.sent"]


@pytest.mark.asyncio
async def test_stream_replays_since_and_filters(seeded: Client, http_client: AsyncClient) -> None:
    await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], "alpha one")
    await _send(seeded, "/beta", "RedStone", ["RedStone"], "beta one")
    await _send(seeded, "/alpha", "GreenCastle", ["BlueLake"], "alpha two")

    everything = _parse_sse((await http_client.get("/mail/api/events", params={"since": 0, "timeout": 0.2})).text)
    assert [e["type"] for e in everything] == ["agent.registered"] * 3 + ["message.sent"] * 3
    assert [e["id"] for e in everything] == sorted(e["id"] for e in everything)
    assert all(e["ts"].endswith("+00:00") for e in everything)

    alpha_sent = _parse_sse(
        (
            await http_client.get(
                "/mail/api/events", params={"since": 0, "project": "alpha", "types": "message.sent", "timeout": 0.2}
            )
        ).text
    )
    assert [(e["project"], e["payload"]["subject"]) for e in alpha_sent] == [("alpha", "alpha one"), ("alpha", "alpha two")]

    # An agent sees what it sent, what was addressed to it, and its own registration.
    red = _parse_sse((await http_client.get("/mail/api/events", params={"since": 0, "agent": "RedStone", "timeout": 0.2})).text)
    assert [(e["type"], e["actor"]) for e in red] == [("agent.registered", "RedStone"), ("message.sent", "RedStone")]

    # Last-Event-ID (sent by EventSource on reconnect) wins over ?since.
    resumed = _parse_sse(
        (
            await http_client.get(
                "/mail/api/events",
                params={"since": 0, "timeout": 0.2},
                headers={"Last-Event-ID": str(everything[-2]["id"])},
            )
        ).text
    )
    assert [e["id"] for e in resumed] == [everything[-1]["id"]]


@pytest.mark.asyncio
async def test_stream_tail_limit_and_live_start(seeded: Client, http_client: AsyncClient) -> None:
    for i in range(3):
        await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], f"m{i}")

    tail = _parse_sse(
        (await http_client.get("/mail/api/events", params={"tail": 2, "types": "message.sent", "timeout": 0.2})).text
    )
    assert [e["payload"]["subject"] for e in tail] == ["m1", "m2"]

    limited = await http_client.get("/mail/api/events", params={"since": 0, "limit": 1})
    assert limited.headers["content-type"].startswith("text/event-stream")
    assert len(_parse_sse(limited.text)) == 1

    # With no cursor and no tail, the stream starts at the end of the log.
    live = await http_client.get("/mail/api/events", params={"timeout": 0.2})
    assert live.status_code == 200
    assert _parse_sse(live.text) == []


@pytest.mark.asyncio
async def test_stream_rejects_unknown_project(http_client: AsyncClient) -> None:
    response = await http_client.get("/mail/api/events", params={"project": "nope", "timeout": 0.1})
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_web_ui_writes_emit_events(seeded: Client, http_client: AsyncClient) -> None:
    first = await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], "one")
    await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], "two")

    marked = await http_client.post("/mail/alpha/inbox/GreenCastle/mark-read", json={"message_ids": [first["id"]]})
    assert marked.json()["marked_count"] == 1
    marked_all = await http_client.post("/mail/alpha/inbox/GreenCastle/mark-all-read")
    assert marked_all.json()["marked_count"] == 1

    overseer = await http_client.post(
        "/mail/alpha/overseer/send", json={"recipients": ["BlueLake"], "subject": "Stop", "body_md": "Pause work."}
    )
    assert overseer.status_code == 200, overseer.text

    tail = [(e.type, e.actor, e.payload.get("subject")) for e in (await _events())[-3:]]
    assert tail == [
        ("message.read", "GreenCastle", "one"),
        ("message.read", "GreenCastle", "two"),
        ("message.sent", "HumanOverseer", "Stop"),
    ]


@pytest.mark.asyncio
async def test_live_page_renders_before_project_routes(http_client: AsyncClient) -> None:
    response = await http_client.get("/mail/live")
    assert response.status_code == 200
    assert "/mail/api/events" in response.text
    assert 'value="alpha"' in response.text


@pytest.mark.asyncio
async def test_share_snapshot_drops_event_log(seeded: Client, tmp_path: Path) -> None:
    await _send(seeded, "/alpha", "BlueLake", ["GreenCastle"], "secret plans")
    db_path = get_database_path(get_settings())
    assert db_path is not None
    snapshot = tmp_path / "snapshot.sqlite3"
    with sqlite3.connect(db_path) as source, sqlite3.connect(snapshot) as dest:
        source.backup(dest)

    # Scoping to one project deletes the other project; its events must not block that.
    apply_project_scope(snapshot, ["alpha"])
    scrub_snapshot(snapshot)
    with sqlite3.connect(snapshot) as conn:
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
