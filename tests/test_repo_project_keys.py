"""Repository project keys: agents on different machines that name the same repo share a project."""
from __future__ import annotations

import json

import pytest
from fastmcp import Client

from mcp_agent_mail.app import _repo_key_from_identifier, _repo_project_slug, build_mcp_server


@pytest.mark.parametrize(
    "identifier",
    [
        "github.com/Owner/Repo",
        "github.com/owner/repo/",
        "https://github.com/owner/repo",
        "https://github.com/owner/repo.git",
        "https://github.com/owner/repo/tree/main/src",
        "git@github.com:owner/repo.git",
        "ssh://git@github.com/owner/repo.git",
        "https://www.github.com/owner/repo",
    ],
)
def test_repo_key_variants_normalize_to_same_key(identifier):
    assert _repo_key_from_identifier(identifier) == "github.com/owner/repo"


@pytest.mark.parametrize(
    "identifier",
    ["/Users/peter/repos/repo", "/backend", "Backend", "backend-abc123", "C:\\projects\\repo", "~/repo", "", None],
)
def test_non_repo_identifiers_are_not_repo_keys(identifier):
    assert _repo_key_from_identifier(identifier) is None


def test_fork_is_a_different_project_from_upstream():
    upstream = _repo_key_from_identifier("github.com/Dicklesworthstone/mcp_agent_mail")
    fork = _repo_key_from_identifier("github.com/peter/mcp_agent_mail")
    assert upstream is not None and fork is not None
    assert upstream != fork
    assert _repo_project_slug(upstream) != _repo_project_slug(fork)


def _data(result):
    if getattr(result, "data", None) is not None:
        return result.data
    return json.loads(result.content[0].text)


@pytest.mark.asyncio
async def test_agents_using_different_repo_spellings_share_a_project(isolated_env):
    server = build_mcp_server()
    async with Client(server) as client:
        a = _data(await client.call_tool("ensure_project", {"human_key": "git@github.com:Owner/Repo.git"}))
        b = _data(await client.call_tool("ensure_project", {"human_key": "https://github.com/owner/repo"}))
        assert a["slug"] == b["slug"] == _repo_project_slug("github.com/owner/repo")
        assert a["human_key"] == "github.com/owner/repo"
        assert a["identity_mode_used"] == "repo"

        # Machine 1 registers with one spelling, machine 2 with another; they can message each other.
        await client.call_tool(
            "register_agent",
            {"project_key": "git@github.com:owner/repo.git", "program": "claude-code", "model": "opus", "name": "BlueLake"},
        )
        await client.call_tool(
            "register_agent",
            {"project_key": "github.com/owner/repo", "program": "gemini-cli", "model": "gemini", "name": "GreenCastle"},
        )
        await client.call_tool(
            "send_message",
            {
                "project_key": "https://github.com/owner/repo",
                "sender_name": "BlueLake",
                "to": ["GreenCastle"],
                "subject": "hello from another machine",
                "body_md": "hi",
            },
        )
        result = await client.call_tool(
            "fetch_inbox", {"project_key": "github.com/owner/repo", "agent_name": "GreenCastle"}
        )
        text = " ".join(getattr(block, "text", "") for block in result.content)
        assert "hello from another machine" in text


@pytest.mark.asyncio
async def test_fork_and_upstream_projects_are_isolated(isolated_env):
    server = build_mcp_server()
    async with Client(server) as client:
        up = _data(await client.call_tool("ensure_project", {"human_key": "github.com/upstream/tool"}))
        fork = _data(await client.call_tool("ensure_project", {"human_key": "github.com/peter/tool"}))
        assert up["slug"] != fork["slug"]


@pytest.mark.asyncio
async def test_unknown_repo_lookup_suggests_ensure_project(isolated_env):
    server = build_mcp_server()
    async with Client(server) as client:
        with pytest.raises(Exception) as exc:
            await client.call_tool(
                "register_agent",
                {"project_key": "github.com/nobody/nothing", "program": "claude-code", "model": "opus", "name": "BlueLake"},
            )
        assert "ensure_project" in str(exc.value)
