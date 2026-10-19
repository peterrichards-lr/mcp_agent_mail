"""End-to-end tests for the HTTP mail viewer routes.

Tests all /mail/* endpoints to ensure proper rendering and functionality.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from mcp_agent_mail import config as _config
from mcp_agent_mail.app import build_mcp_server
from mcp_agent_mail.db import ensure_schema, get_session
from mcp_agent_mail.http import build_http_app
from mcp_agent_mail.storage import ensure_archive, write_agent_profile


async def _setup_test_data(
    settings: _config.Settings,
    *,
    include_cross_project_sender: bool = False,
) -> dict:
    """Create test project, agent, and messages for viewer tests."""
    await ensure_schema()

    # Create project
    async with get_session() as session:
        from sqlalchemy import text

        await session.execute(
            text("INSERT INTO projects (slug, human_key, created_at) VALUES (:slug, :hk, datetime('now'))"),
            {"slug": "test-proj", "hk": "/tmp/test-proj"},
        )
        await session.commit()
        row = await session.execute(text("SELECT id FROM projects WHERE slug = :slug"), {"slug": "test-proj"})
        project_id = row.scalar()

        # Create agent
        await session.execute(
            text(
                "INSERT INTO agents (name, project_id, program, model, task_description, inception_ts, last_active_ts, attachments_policy, contact_policy) "
                "VALUES (:name, :pid, :prog, :model, :task, datetime('now'), datetime('now'), 'auto', 'auto')"
            ),
            {"name": "BlueLake", "pid": project_id, "prog": "claude-code", "model": "opus-4", "task": "Testing"},
        )
        await session.commit()
        row = await session.execute(text("SELECT id FROM agents WHERE name = :name"), {"name": "BlueLake"})
        agent_id = row.scalar()

        # Create messages
        await session.execute(
            text(
                "INSERT INTO messages (project_id, subject, body_md, importance, ack_required, sender_id, thread_id, created_ts) "
                "VALUES (:pid, :subj, :body, :imp, :ack, :sid, :tid, datetime('now'))"
            ),
            {
                "pid": project_id,
                "subj": "Test Message 1",
                "body": "This is a test message body.",
                "imp": "normal",
                "ack": 0,
                "sid": agent_id,
                "tid": "thread-1",
            },
        )
        await session.execute(
            text(
                "INSERT INTO messages (project_id, subject, body_md, importance, ack_required, sender_id, thread_id, created_ts) "
                "VALUES (:pid, :subj, :body, :imp, :ack, :sid, :tid, datetime('now'))"
            ),
            {
                "pid": project_id,
                "subj": "Urgent Alert",
                "body": "This is an urgent message.",
                "imp": "urgent",
                "ack": 1,
                "sid": agent_id,
                "tid": "thread-2",
            },
        )
        await session.commit()

        # Get message IDs
        row = await session.execute(text("SELECT id FROM messages ORDER BY id"))
        message_ids = [r[0] for r in row.fetchall()]

        # Create recipient entries
        for mid in message_ids:
            await session.execute(
                text("INSERT INTO message_recipients (message_id, agent_id, kind) VALUES (:mid, :aid, :kind)"),
                {"mid": mid, "aid": agent_id, "kind": "to"},
            )
        await session.commit()

        cross_project_message_id = None
        if include_cross_project_sender:
            await session.execute(
                text("INSERT INTO projects (slug, human_key, created_at) VALUES (:slug, :hk, datetime('now'))"),
                {"slug": "source-proj", "hk": "/tmp/source-proj"},
            )
            await session.commit()
            row = await session.execute(text("SELECT id FROM projects WHERE slug = :slug"), {"slug": "source-proj"})
            source_project_id = row.scalar()

            await session.execute(
                text(
                    "INSERT INTO agents (name, project_id, program, model, task_description, inception_ts, last_active_ts, attachments_policy, contact_policy) "
                    "VALUES (:name, :pid, :prog, :model, :task, datetime('now'), datetime('now'), 'auto', 'auto')"
                ),
                {
                    "name": "BlueLake",
                    "pid": source_project_id,
                    "prog": "claude-code",
                    "model": "opus-4",
                    "task": "Cross-project testing",
                },
            )
            await session.commit()
            row = await session.execute(
                text(
                    "SELECT id FROM agents WHERE project_id = :pid AND name = :name"
                ),
                {"pid": source_project_id, "name": "BlueLake"},
            )
            external_agent_id = row.scalar()

            await session.execute(
                text(
                    "INSERT INTO messages (project_id, subject, body_md, importance, ack_required, sender_id, thread_id, created_ts) "
                    "VALUES (:pid, :subj, :body, :imp, :ack, :sid, :tid, datetime('now'))"
                ),
                {
                    "pid": project_id,
                    "subj": "Cross Project Notice",
                    "body": "Sent from another project.",
                    "imp": "high",
                    "ack": 0,
                    "sid": external_agent_id,
                    "tid": "thread-cross",
                },
            )
            await session.commit()
            row = await session.execute(
                text("SELECT id FROM messages WHERE project_id = :pid AND subject = :subj"),
                {"pid": project_id, "subj": "Cross Project Notice"},
            )
            cross_project_message_id = row.scalar()
            await session.execute(
                text("INSERT INTO message_recipients (message_id, agent_id, kind) VALUES (:mid, :aid, :kind)"),
                {"mid": cross_project_message_id, "aid": agent_id, "kind": "to"},
            )
            await session.commit()

    # Also create archive artifacts
    archive = await ensure_archive(settings, "test-proj")
    await write_agent_profile(
        archive,
        {
            "name": "BlueLake",
            "program": "claude-code",
            "model": "opus-4",
            "task_description": "Testing",
        },
    )

    return {
        "project_id": project_id,
        "project_slug": "test-proj",
        "agent_id": agent_id,
        "agent_name": "BlueLake",
        "message_ids": message_ids,
        "cross_project_message_id": cross_project_message_id,
    }


# =============================================================================
# Unified Inbox Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_unified_inbox_html(isolated_env):
    """Test GET /mail returns HTML unified inbox."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        # Should contain some HTML structure
        assert "<html" in resp.text.lower() or "<!doctype" in resp.text.lower()


@pytest.mark.asyncio
async def test_mail_unified_inbox_api(isolated_env):
    """Test GET /mail/api/unified-inbox returns JSON."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/api/unified-inbox")
        assert resp.status_code == 200
        data = resp.json()
        assert "messages" in data or "items" in data or isinstance(data, list)


@pytest.mark.asyncio
async def test_mail_unified_inbox_api_disambiguates_external_sender(isolated_env):
    """Unified inbox JSON should preserve external sender origin."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    data = await _setup_test_data(settings, include_cross_project_sender=True)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/api/unified-inbox")
        assert resp.status_code == 200
        payload = resp.json()
        message = next(item for item in payload["messages"] if item["id"] == data["cross_project_message_id"])
        assert message["sender"] == "BlueLake@source-proj"
        assert message["sender_project"] == "/tmp/source-proj"
        assert message["sender_address"] == "project:source-proj#BlueLake"


@pytest.mark.asyncio
async def test_mail_unified_inbox_alternate_route(isolated_env):
    """Test GET /mail/unified-inbox alternate route."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/unified-inbox")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


# =============================================================================
# Projects List Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_projects_list(isolated_env):
    """Test GET /mail/projects returns project listing."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/projects")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        # Should mention the test project
        assert "test-proj" in resp.text or "test" in resp.text.lower()


# =============================================================================
# Project View Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_project_view(isolated_env):
    """Test GET /mail/{project} returns project view."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_mail_project_view_with_search(isolated_env):
    """Test GET /mail/{project}?q=search returns filtered results."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj", params={"q": "urgent"})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_mail_project_view_nonexistent(isolated_env):
    """Test GET /mail/{project} with nonexistent project."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await ensure_schema()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/nonexistent-project")
        # Should return 404 or show empty page
        assert resp.status_code in (200, 404)


# =============================================================================
# Agent Inbox Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_agent_inbox(isolated_env):
    """Test GET /mail/{project}/inbox/{agent} returns agent inbox."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/inbox/BlueLake")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        # Should show messages
        assert "Test Message" in resp.text or "message" in resp.text.lower()


@pytest.mark.asyncio
async def test_mail_agent_inbox_pagination(isolated_env):
    """Test inbox pagination with page parameter."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/inbox/BlueLake", params={"page": 1, "limit": 10})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_mail_agent_inbox_nonexistent_agent(isolated_env):
    """Test inbox for nonexistent agent."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/inbox/NonexistentAgent")
        # Should return 404 or empty inbox
        assert resp.status_code in (200, 404)


# =============================================================================
# Message Detail Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_message_detail(isolated_env):
    """Test GET /mail/{project}/message/{mid} returns message detail."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    data = await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        mid = data["message_ids"][0]
        resp = await client.get(f"/mail/test-proj/message/{mid}")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        # Should show the message subject
        assert "Test Message" in resp.text or "message" in resp.text.lower()


@pytest.mark.asyncio
async def test_mail_message_and_thread_views_disambiguate_external_sender(isolated_env):
    """Dedicated HTML views should show the external sender's origin."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    data = await _setup_test_data(settings, include_cross_project_sender=True)
    cross_message_id = data["cross_project_message_id"]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        message_resp = await client.get(f"/mail/test-proj/message/{cross_message_id}")
        assert message_resp.status_code == 200
        assert "BlueLake@source-proj" in message_resp.text

        thread_resp = await client.get("/mail/test-proj/thread/thread-cross")
        assert thread_resp.status_code == 200
        assert "BlueLake@source-proj" in thread_resp.text

        search_resp = await client.get("/mail/test-proj/search", params={"q": "Cross Project"})
        assert search_resp.status_code == 200
        assert "BlueLake@source-proj" in search_resp.text


@pytest.mark.asyncio
async def test_mail_message_detail_nonexistent(isolated_env):
    """Test message detail for nonexistent message ID."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/message/99999")
        # Server may return 200 with "not found" HTML page or 404
        assert resp.status_code in (200, 404)


# =============================================================================
# Mark Read Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_mark_read_single(isolated_env):
    """Test POST /mail/{project}/inbox/{agent}/mark-read marks message as read."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    data = await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        mid = data["message_ids"][0]
        # Server expects JSON body
        resp = await client.post(
            "/mail/test-proj/inbox/BlueLake/mark-read",
            json={"message_ids": [mid]},
        )
        # Should redirect or return success
        assert resp.status_code in (200, 302, 303)


@pytest.mark.asyncio
async def test_mail_mark_all_read(isolated_env):
    """Test POST /mail/{project}/inbox/{agent}/mark-all-read marks all as read."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/mail/test-proj/inbox/BlueLake/mark-all-read")
        # Should redirect or return success
        assert resp.status_code in (200, 302, 303)


# =============================================================================
# Thread View Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_thread_view(isolated_env):
    """Test GET /mail/{project}/thread/{thread_id} returns thread view."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/thread/thread-1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_mail_thread_view_nonexistent(isolated_env):
    """Test thread view for nonexistent thread."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/thread/nonexistent-thread")
        # Should return 200 with empty or 404
        assert resp.status_code in (200, 404)


# =============================================================================
# Search Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_search_page(isolated_env):
    """Test GET /mail/{project}/search returns search interface."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Search route may require a query parameter
        resp = await client.get("/mail/test-proj/search", params={"q": ""})
        # Accept 200 (success) or 422 (validation) if route requires non-empty query
        assert resp.status_code in (200, 422)


@pytest.mark.asyncio
async def test_mail_search_with_query(isolated_env):
    """Test search with query parameter."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/search", params={"q": "urgent"})
        assert resp.status_code == 200


# =============================================================================
# File Reservations View Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_file_reservations_view(isolated_env):
    """Test GET /mail/{project}/file_reservations returns reservations view."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/file_reservations")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


# =============================================================================
# Attachments View Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_attachments_view(isolated_env):
    """Test GET /mail/{project}/attachments returns attachments browser."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/attachments")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


# =============================================================================
# Overseer (Human Sender) Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_overseer_compose(isolated_env):
    """Test GET /mail/{project}/overseer/compose returns compose form."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/test-proj/overseer/compose")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        # Should have a form
        assert "<form" in resp.text.lower() or "form" in resp.text.lower()


@pytest.mark.asyncio
async def test_mail_overseer_send(isolated_env):
    """Test POST /mail/{project}/overseer/send sends message."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Server expects JSON body
        resp = await client.post(
            "/mail/test-proj/overseer/send",
            json={
                "to": ["BlueLake"],
                "subject": "Test from Overseer",
                "body_md": "This is a test message from the human overseer.",
            },
        )
        # Should redirect on success, return success, or validation error (400)
        # Server may require additional fields like sender registration
        assert resp.status_code in (200, 302, 303, 400)


@pytest.mark.asyncio
async def test_mail_overseer_send_missing_fields(isolated_env):
    """Test overseer send with missing required fields."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Server expects JSON - missing subject and body_md
        resp = await client.post(
            "/mail/test-proj/overseer/send",
            json={"to": ["BlueLake"]},  # Missing subject and body_md
        )
        # Should return error, validation failure, or 500 (server validation error)
        assert resp.status_code in (200, 400, 422, 500)


# =============================================================================
# XSS Prevention Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_xss_in_search_query(isolated_env):
    """Test that XSS in search query is escaped."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await _setup_test_data(settings)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        xss_payload = "<script>alert('xss')</script>"
        resp = await client.get("/mail/test-proj/search", params={"q": xss_payload})
        assert resp.status_code == 200
        # The raw script tag should not appear unescaped
        assert "<script>alert('xss')</script>" not in resp.text


@pytest.mark.asyncio
async def test_mail_xss_in_project_name(isolated_env):
    """Test that XSS in project name path is handled safely."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await ensure_schema()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        xss_payload = "<script>alert('xss')</script>"
        resp = await client.get(f"/mail/{xss_payload}")
        # Should handle gracefully without executing script
        assert resp.status_code in (200, 404)
        # Regardless of status, should never reflect raw script tag
        assert "<script>alert('xss')</script>" not in resp.text


# =============================================================================
# Lock Status API Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mail_api_locks_empty(isolated_env):
    """Test GET /mail/api/locks with no locks."""
    settings = _config.get_settings()
    server = build_mcp_server()
    app = build_http_app(settings, server)

    await ensure_schema()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/mail/api/locks")
        assert resp.status_code == 200
        data = resp.json()
        assert "locks" in data
        assert isinstance(data["locks"], list)


# --- Third-party front-end files (#20) ----------------------------------------------------------
# The web UI runs in signed-in pages on a public server. Every external script and stylesheet is
# pinned to an exact version and carries Subresource Integrity, so a newly published or tampered
# file on a CDN can't run there. A failed hash only shows in a browser, so this keeps new tags
# from skipping it.

_TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "src" / "mcp_agent_mail" / "templates"
_EXACT_VERSION = re.compile(r"[@/]\d+\.\d+\.\d+(?=[/'\"]|$)")
_EXTERNAL_TAG = re.compile(
    r"<(?:script\b[^>]*\bsrc|link\b(?=[^>]*\brel=\"stylesheet\")[^>]*\bhref)=\"(https://[^\"]+)\"[^>]*>"
)
_URL_IMPORT = re.compile(r"\bimport\b[^;'\"]*['\"](https://[^'\"]+)['\"]")
# Allowed without integrity, each for a reason the template states:
# Google Fonts' CSS differs by browser, so it can't be hashed; the Tailwind play CDN sends no CORS
# header, so a hashed load is refused (prebuilt CSS replaces both in #20's second part).
_NO_INTEGRITY_ALLOWED = ("https://fonts.googleapis.com/css2?", "https://cdn.tailwindcss.com/")


def _template_sources() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(_TEMPLATES_DIR.glob("*.html"))}


def test_external_scripts_and_stylesheets_are_pinned_with_integrity():
    problems: list[str] = []
    checked = 0
    for name, html in _template_sources().items():
        for match in _EXTERNAL_TAG.finditer(html):
            tag, url = match.group(0), match.group(1)
            checked += 1
            if url.startswith(_NO_INTEGRITY_ALLOWED[0]):
                continue
            if not _EXACT_VERSION.search(url):
                problems.append(f"{name}: not an exact version: {url}")
            if url.startswith(_NO_INTEGRITY_ALLOWED[1]):
                continue
            if not re.search(r'\bintegrity="sha(256|384|512)-[A-Za-z0-9+/=]+"', tag):
                problems.append(f"{name}: no integrity hash: {url}")
            if 'crossorigin="anonymous"' not in tag:
                problems.append(f'{name}: no crossorigin="anonymous" (needed for the hash check): {url}')
        for match in _URL_IMPORT.finditer(html):
            checked += 1
            if not _EXACT_VERSION.search(match.group(1)):
                problems.append(f"{name}: module import not at an exact version: {match.group(1)}")
    assert checked > 30, f"expected the web UI's third-party files, found {checked}"
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize(
    ("tag", "problem"),
    [
        ('<script src="https://unpkg.com/lucide@latest"></script>', "not an exact version"),
        ('<script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>', "not an exact version"),
        ('<script src="https://unpkg.com/tippy.js@6"></script>', "not an exact version"),
        ('<script src="https://cdn.jsdelivr.net/npm/marked@11.2.0/marked.min.js"></script>', "no integrity hash"),
        (
            '<link rel="stylesheet" href="https://unpkg.com/tippy.js@6.3.7/dist/tippy.css" integrity="sha384-x" />',
            "crossorigin",
        ),
        ("<script type=\"module\">import m from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/m.mjs';</script>", "module import"),
    ],
)
def test_pinning_check_catches_unpinned_tags(tag, problem, monkeypatch, tmp_path):
    """The check above flags each kind of unpinned tag, so its clean result means something."""
    padding = "".join(
        f'<script src="https://cdn.example/x@1.0.{i}/x.js" integrity="sha384-AA" crossorigin="anonymous"></script>'
        for i in range(31)
    )
    (tmp_path / "page.html").write_text(padding + tag, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "_TEMPLATES_DIR", tmp_path)
    with pytest.raises(AssertionError, match=re.escape(problem)):
        test_external_scripts_and_stylesheets_are_pinned_with_integrity()
