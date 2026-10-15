"""Presence: heartbeats, who_is_online, set_status, the offline sweep and their live events."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest
from fastmcp import Client
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlmodel import col, select

from mcp_agent_mail.app import _naive_utc, build_mcp_server, sweep_presence
from mcp_agent_mail.config import get_settings
from mcp_agent_mail.db import get_session
from mcp_agent_mail.http import build_http_app
from mcp_agent_mail.models import Event

PROJECT = "github.com/owner/presence"


@pytest.fixture
async def mcp(isolated_env) -> AsyncIterator[tuple[Client, dict[str, str]]]:
    async with Client(build_mcp_server()) as client:
        await client.call_tool("ensure_project", {"human_key": PROJECT})
        tokens = {}
        for name in ("BlueLake", "GreenCastle", "RedStone"):
            result = await client.call_tool(
                "register_agent", {"project_key": PROJECT, "program": "claude-code", "model": "opus", "name": name}
            )
            tokens[name] = result.data["registration_token"]
        yield client, tokens


@pytest.fixture
async def http(mcp: tuple[Client, dict[str, str]]) -> AsyncIterator[AsyncClient]:
    app = build_http_app(get_settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def _beat(http: AsyncClient, agent: str, **fields: Any) -> dict[str, Any]:
    response = await http.post("/mail/api/presence", json={"project": PROJECT, "agent": agent, **fields})
    assert response.status_code == 200, response.text
    return response.json()


async def _presence_events() -> list[tuple[str, str, bool]]:
    async with get_session() as session:
        rows = (await session.execute(select(Event).where(col(Event.type) == "agent.presence").order_by(col(Event.id)))).scalars().all()
    return [(e.actor, e.payload["status"], e.payload["wakeable"]) for e in rows]


async def _age(agent: str, seconds: int) -> None:
    """Pretend the agent's last signs of life happened `seconds` ago."""
    past = _naive_utc() - timedelta(seconds=seconds)
    async with get_session() as session:
        await session.execute(
            text(
                "UPDATE agent_presence SET last_seen_ts = :t, last_listener_ts = CASE WHEN last_listener_ts IS NULL "
                "THEN NULL ELSE :t END WHERE agent_id = (SELECT id FROM agents WHERE name = :a)"
            ),
            {"t": past, "a": agent},
        )
        await session.execute(text("UPDATE agents SET last_active_ts = :t WHERE name = :a"), {"t": past, "a": agent})
        await session.commit()


async def _online(client: Client, **args: Any) -> dict[str, dict[str, Any]]:
    result = await client.call_tool("who_is_online", {"project_key": PROJECT, **args})
    return {a["agent"]: a for a in result.data["agents"]}


@pytest.mark.asyncio
async def test_listener_heartbeat_makes_an_agent_idle_and_wakeable(mcp, http: AsyncClient) -> None:
    client, _ = mcp
    view = await _beat(http, "GreenCastle", state="idle", listener=True, machine="laptop", client="claude-code", session="s1")
    assert (view["status"], view["wakeable"], view["machine"], view["client"]) == ("idle", True, "laptop", "claude-code")

    # Plain heartbeats keep it online without new events.
    await _beat(http, "GreenCastle", listener=True)
    await _beat(http, "GreenCastle", listener=True)
    assert await _presence_events() == [("GreenCastle", "idle", True)]

    online = await _online(client)
    assert online["GreenCastle"]["status"] == "idle" and online["GreenCastle"]["wakeable"] is True
    assert online["GreenCastle"]["machine"] == "laptop"


@pytest.mark.asyncio
async def test_turns_switch_between_busy_and_idle(mcp, http: AsyncClient) -> None:
    client, _ = mcp
    await _beat(http, "GreenCastle", state="idle", listener=True)
    await _beat(http, "GreenCastle", state="busy", listener=False)  # woken by mail
    await _beat(http, "GreenCastle", state="idle", listener=True)  # turn over, listening again
    assert await _presence_events() == [
        ("GreenCastle", "idle", True),
        ("GreenCastle", "busy", False),
        ("GreenCastle", "idle", True),
    ]
    assert (await _online(client))["GreenCastle"]["status"] == "idle"


@pytest.mark.asyncio
async def test_sweep_marks_silent_agents_offline_once(mcp, http: AsyncClient) -> None:
    client, _ = mcp
    await _beat(http, "GreenCastle", state="idle", listener=True)
    await _beat(http, "RedStone", state="busy")

    # The listener missed its heartbeats: no longer wakeable, but still idle within the idle TTL.
    await _age("GreenCastle", 200)
    assert (await _online(client))["GreenCastle"] | {"last_seen": None, "status_since": None} == {
        "agent": "GreenCastle", "status": "idle", "wakeable": False, "machine": "", "client": "",
        "working_on": "", "last_seen": None, "status_since": None,
    }
    assert await sweep_presence() == 0

    # Long silence: offline. A busy agent gets longer (a turn's tool calls can run for many minutes).
    await _age("GreenCastle", 11 * 60)
    await _age("RedStone", 11 * 60)
    assert await sweep_presence() == 1
    assert await sweep_presence() == 0
    online = await _online(client)
    assert "GreenCastle" not in online and online["RedStone"]["status"] == "busy"
    assert (await _online(client, include_offline=True))["GreenCastle"]["status"] == "offline"
    assert (await _presence_events())[-1] == ("GreenCastle", "offline", False)

    await _age("RedStone", 31 * 60)
    assert await sweep_presence() == 1


@pytest.mark.asyncio
async def test_agents_without_hooks_count_through_their_own_calls(mcp) -> None:
    client, _ = mcp
    online = await _online(client)  # all three just registered
    assert {a: v["status"] for a, v in online.items()} == {"BlueLake": "idle", "GreenCastle": "idle", "RedStone": "idle"}
    assert not any(v["wakeable"] for v in online.values())

    await _age("BlueLake", 11 * 60)
    assert "BlueLake" not in await _online(client)


@pytest.mark.asyncio
async def test_set_status_is_what_others_see(mcp) -> None:
    client, tokens = mcp
    await client.call_tool(
        "set_status",
        {"project_key": PROJECT, "agent_name": "BlueLake", "working_on": "Fixing the flaky login test, br-42",
         "registration_token": tokens["BlueLake"]},
    )
    assert (await _online(client))["BlueLake"]["working_on"] == "Fixing the flaky login test, br-42"
    async with get_session() as session:
        event = (await session.execute(select(Event).where(col(Event.type) == "agent.status"))).scalars().one()
    assert (event.actor, event.payload) == ("BlueLake", {"working_on": "Fixing the flaky login test, br-42"})


@pytest.mark.asyncio
async def test_heartbeat_validation(http: AsyncClient) -> None:
    assert (await http.post("/mail/api/presence", json={"project": PROJECT})).status_code == 400
    assert (await http.post("/mail/api/presence", json={"project": PROJECT, "agent": "Nobody"})).status_code == 404
    bad = await http.post("/mail/api/presence", json={"project": PROJECT, "agent": "BlueLake", "state": "asleep"})
    assert bad.status_code == 400 and "asleep" in bad.text

    listing = await http.get("/mail/api/presence", params={"project": PROJECT})
    assert listing.status_code == 200 and {a["agent"] for a in listing.json()["agents"]} >= {"BlueLake"}
    page = await http.get("/mail/live")
    assert "Online now" in page.text
