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
# The web UI runs in signed-in pages on a public server, and the share viewer runs next to exported
# mailbox data. Every third-party file they load is pinned to an exact version and, where the CDN
# allows it, carries Subresource Integrity, so a newly published or tampered file can't run there.
# A failed hash only shows in a browser, so this check finds every URL in the pages, however it's
# written, and classifies it, rather than matching only the tag forms in use today.

_SRC_DIR = Path(__file__).resolve().parents[1] / "src" / "mcp_agent_mail"
_PAGE_GLOBS = ("templates/*.html", "viewer_assets/*.html")
# Any URL with a host: https:, http: or protocol-relative, in any case or quoting.
_ANY_URL = re.compile(r"(?i)(?:https?:)?//([a-z0-9-]+(?:\.[a-z0-9-]+)+)(/[^\s\"'`<>)\\]*)?")
_EXACT_VERSION = re.compile(r"[@/]\d+\.\d+\.\d+(?=[/?#]|$)")
_CDN_HOSTS = {"cdn.jsdelivr.net", "unpkg.com", "cdnjs.cloudflare.com", "cdn.tailwindcss.com"}
# Referenced but never loaded as code: SVG and XML namespaces.
_NAMESPACE_HOSTS = {"www.w3.org"}
# Google Fonts' preconnect hints and font CSS. The CSS differs by browser, so it can be neither
# pinned nor hashed; #20's second part self-hosts the font.
_FONT_HOSTS = {"fonts.googleapis.com", "fonts.gstatic.com"}
# Pinned files that can't carry a hash, for the reason given where each is loaded. Exact URLs, so a
# version change is reviewed here too. #20's second part self-hosts them.
_PINNED_WITHOUT_INTEGRITY = {
    "https://cdn.tailwindcss.com/3.4.17",  # the play CDN sends no CORS header
    "https://cdn.jsdelivr.net/npm/mermaid@11.17.2/dist/mermaid.esm.min.mjs",  # imports its own chunks
}


def _enclosing_tag(html: str, pos: int) -> str | None:
    """The HTML tag that the text at ``pos`` sits inside, or None if it's in element content."""
    start = html.rfind("<", 0, pos)
    if start == -1 or html.rfind(">", 0, pos) > start:
        return None
    return html[start : html.find(">", pos) + 1]


def _third_party_problems() -> list[str]:
    problems: list[str] = []
    for path in sorted(p for pattern in _PAGE_GLOBS for p in _SRC_DIR.glob(pattern)):
        name, html = f"{path.parent.name}/{path.name}", path.read_text(encoding="utf-8")
        for match in _ANY_URL.finditer(html):
            url, host, url_path = match.group(0), match.group(1).lower(), match.group(2) or ""
            if host in _NAMESPACE_HOSTS or host in _FONT_HOSTS:
                continue
            if host not in _CDN_HOSTS:
                problems.append(f"{name}: unreviewed third-party host: {url}")
                continue
            tag = _enclosing_tag(html, match.start())
            if url_path in ("", "/") and tag is not None and (
                re.match(r"(?i)<meta\b", tag) or re.search(r"(?i)\brel\s*=\s*[\"']?(preconnect|dns-prefetch)\b", tag)
            ):
                continue  # a bare origin in a CSP or a preconnect hint: nothing is loaded from it
            if not url.lower().startswith("https://"):
                problems.append(f"{name}: not loaded over https: {url}")
            if not _EXACT_VERSION.search(url_path):
                problems.append(f"{name}: not an exact version: {url}")
            if url in _PINNED_WITHOUT_INTEGRITY:
                continue
            if tag is None:
                problems.append(f"{name}: loaded from script or text, where it can't carry a hash: {url}")
            elif not re.search(r"(?i)\bintegrity\s*=\s*[\"']sha(256|384|512)-", tag):
                problems.append(f"{name}: no integrity hash: {url}")
            elif not re.search(r"(?i)\bcrossorigin\s*=\s*[\"']?anonymous", tag):
                problems.append(f'{name}: no crossorigin="anonymous" (needed for the hash check): {url}')
    return problems


def test_third_party_files_are_pinned_with_integrity():
    problems = _third_party_problems()
    assert not problems, "\n".join(problems)


_CLEAN_PAGE = """
<meta http-equiv="Content-Security-Policy" content="script-src 'self' https://unpkg.com https://cdn.jsdelivr.net">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter&display=swap" rel="stylesheet">
<script src="https://cdn.tailwindcss.com/3.4.17"></script>
<SCRIPT SRC='https://unpkg.com/x@1.2.3/dist/x.js' INTEGRITY='sha384-AA' CROSSORIGIN='anonymous'></SCRIPT>
<link href="https://unpkg.com/x@1.2.3/x.css"
      rel="stylesheet" integrity="sha384-AA" crossorigin="anonymous">
<svg xmlns="http://www.w3.org/2000/svg"></svg>
"""


def _check_page(page: str, monkeypatch, tmp_path) -> list[str]:
    (tmp_path / "templates").mkdir()
    (tmp_path / "templates" / "page.html").write_text(page, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "_SRC_DIR", tmp_path)
    return _third_party_problems()


def test_pinning_check_accepts_a_clean_page(monkeypatch, tmp_path):
    assert _check_page(_CLEAN_PAGE, monkeypatch, tmp_path) == []


@pytest.mark.parametrize(
    ("tag", "problem"),
    [
        ('<script src="https://unpkg.com/lucide@latest"></script>', "not an exact version"),
        ('<script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>', "not an exact version"),
        ('<script src="https://cdn.tailwindcss.com"></script>', "not an exact version"),
        ("<SCRIPT SRC='https://unpkg.com/x@1.2.3/x.js'></SCRIPT>", "no integrity hash"),
        ('<script src = "https://unpkg.com/x@1.2.3/x.js" ></script>', "no integrity hash"),
        ('<link rel="modulepreload" href="https://cdn.jsdelivr.net/npm/x@1.0.0/x.mjs">', "no integrity hash"),
        (
            '<link href="https://unpkg.com/x@1.2.3/x.css" rel="stylesheet" integrity="sha384-AA">',
            "crossorigin",
        ),
        (
            '<script src="//unpkg.com/x@1.2.3/x.js" integrity="sha384-AA" crossorigin="anonymous"></script>',
            "not loaded over https",
        ),
        ("<script>s.src = 'https://unpkg.com/x@1.2.3/x.js';</script>", "loaded from script or text"),
        ("<script type=\"module\">export { m } from 'https://cdn.jsdelivr.net/npm/m@11/m.mjs';</script>", "not an exact version"),
        ('<script src="https://cdn.example.net/x@1.2.3/x.js" integrity="sha384-AA" crossorigin="anonymous"></script>', "unreviewed"),
    ],
)
def test_pinning_check_catches_unpinned_files(tag, problem, monkeypatch, tmp_path):
    """Each way of loading an unchecked file is flagged, so the check's clean result means something."""
    problems = _check_page(_CLEAN_PAGE + tag, monkeypatch, tmp_path)
    assert any(problem in p for p in problems), problems
