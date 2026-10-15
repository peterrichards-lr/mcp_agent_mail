"""Security audit log: sign-ins, sessions, tokens, accounts and destructive web-UI actions.

Agent activity (mail sent, read, presence) lives in the ``events`` table; this
is the record of what *people* and operators did. Pattern from lfr-tunnel's
``writeAudit``, including its lesson: behind a reverse proxy the socket peer is
the proxy, so the IP recorded is ``request.client.host`` as rewritten by
uvicorn's trusted-proxy handling (FORWARDED_ALLOW_IPS), not the raw peer.

Recording never raises: a failure to audit is logged, and the action it
describes goes ahead.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlmodel import delete, select

from .db import ensure_schema, get_session
from .models import AuditEntry

if TYPE_CHECKING:
    from starlette.requests import Request

logger = logging.getLogger(__name__)


def _client(request: Request | None) -> tuple[str, str]:
    if request is None:
        return "", ""
    host = request.client.host if request.client else ""
    return host[:64], request.headers.get("user-agent", "")[:200]


async def record(
    action: str,
    *,
    actor: str,
    target_type: str = "",
    target_id: str = "",
    details: str = "",
    request: Request | None = None,
) -> None:
    """Append one entry. ``actor`` is a session user, a token owner, ``cli`` or what was typed at sign-in."""
    ip, user_agent = _client(request)
    try:
        await ensure_schema()
        async with get_session() as session:
            session.add(
                AuditEntry(
                    actor=(actor or "anonymous")[:128],
                    action=action[:64],
                    target_type=target_type[:32],
                    target_id=str(target_id)[:256],
                    details=details[:2048],
                    ip=ip,
                    user_agent=user_agent,
                )
            )
            await session.commit()
    except Exception:
        logger.exception("audit: failed to record %s by %s", action, actor)


async def recent(*, limit: int = 200, actor: str = "", action_prefix: str = "") -> list[AuditEntry]:
    await ensure_schema()
    query = select(AuditEntry)
    if actor:
        query = query.where(AuditEntry.actor == actor)
    if action_prefix:
        query = query.where(cast(Any, AuditEntry.action).startswith(action_prefix))
    async with get_session() as session:
        rows = await session.execute(query.order_by(cast(Any, AuditEntry.id).desc()).limit(max(1, min(limit, 1000))))
        return list(rows.scalars().all())


async def prune(retention_days: int) -> int:
    """Delete entries older than the retention period; returns how many."""
    if retention_days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=retention_days)
    await ensure_schema()
    async with get_session() as session:
        result = await session.execute(delete(AuditEntry).where(cast(Any, AuditEntry.ts) < cutoff))
        await session.commit()
        return int(getattr(result, "rowcount", 0) or 0)
