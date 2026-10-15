"""Per-IP throttling of sign-in attempts and temporary bans for credential guessing.

The per-account lockout in ui_auth.py stops someone hammering one account; it
does nothing against one password tried across many usernames, or a scanner
cycling bearer tokens. Pattern from lfr-tunnel (rate_limit.go, blacklist.go):

- sign-in: SIGNIN_LIMIT failed sign-ins (any method) from one IP within
  SIGNIN_WINDOW block further sign-ins from it until the window passes;
- credentials: CREDENTIAL_LIMIT rejected credentials (bad bearer tokens,
  unauthenticated API/MCP calls) within CREDENTIAL_WINDOW ban the IP from the
  whole server for BAN_DURATION.

State is in the database, so every worker sees it; ban checks are cached
briefly per worker.

The IP is the one uvicorn reports after its trusted-proxy handling
(FORWARDED_ALLOW_IPS). If a request carries X-Forwarded-For that uvicorn did
*not* trust, every client would look like the proxy and one ban would lock
everybody out, so throttling stands down for such requests instead. Loopback
addresses and HTTP_AUTH_THROTTLE_EXEMPT_IPS are never throttled.
"""

from __future__ import annotations

import ipaddress
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlmodel import delete, func, select

from .config import get_settings
from .db import ensure_schema, get_session
from .models import AuthFailure, IpBan

if TYPE_CHECKING:
    from starlette.requests import Request

logger = logging.getLogger(__name__)

SIGNIN_LIMIT = 10
SIGNIN_WINDOW = timedelta(minutes=15)
CREDENTIAL_LIMIT = 50
CREDENTIAL_WINDOW = timedelta(minutes=10)
BAN_DURATION = timedelta(minutes=60)
_KEEP_FAILURES = timedelta(days=1)
_BAN_CACHE_SECONDS = 15.0
_ban_cache: dict[str, tuple[datetime | None, float]] = {}
_warned_untrusted_proxy = False


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def client_ip(request: Request) -> str | None:
    """The IP to throttle for this request, or None when it must not be throttled."""
    global _warned_untrusted_proxy
    http_settings = get_settings().http
    if not http_settings.auth_throttle_enabled or request.client is None:
        return None
    ip = request.client.host
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded and ip not in {part.strip() for part in forwarded.split(",")}:
        if not _warned_untrusted_proxy:
            logger.warning(
                "ip_guard: X-Forwarded-For from untrusted proxy %s; set FORWARDED_ALLOW_IPS so clients' real "
                "IPs are seen. Throttling is off for these requests.", ip,
            )
            _warned_untrusted_proxy = True
        return None
    try:
        if ipaddress.ip_address(ip).is_loopback:
            return None
    except ValueError:
        return None
    if ip in http_settings.auth_throttle_exempt_ips:
        return None
    return ip


async def banned_until(ip: str) -> datetime | None:
    """When the IP's ban ends, if it is banned now."""
    cached = _ban_cache.get(ip)
    if cached is not None and time.monotonic() - cached[1] < _BAN_CACHE_SECONDS:
        until = cached[0]
    else:
        await ensure_schema()
        async with get_session() as session:
            row = await session.get(IpBan, ip)
        until = row.until_ts if row is not None else None
        _ban_cache[ip] = (until, time.monotonic())
    return until if until is not None and until > _now() else None


async def _recent_failures(ip: str, kind: str, window: timedelta) -> int:
    async with get_session() as session:
        result = await session.execute(
            select(func.count())
            .select_from(AuthFailure)
            .where(AuthFailure.ip == ip, AuthFailure.kind == kind, cast(Any, AuthFailure.ts) > _now() - window)
        )
        return int(result.scalar_one())


async def signin_blocked(ip: str) -> bool:
    """Whether sign-ins from this IP are paused (too many recent failures)."""
    await ensure_schema()
    return await _recent_failures(ip, "signin", SIGNIN_WINDOW) >= SIGNIN_LIMIT


async def record_failure(ip: str, kind: str) -> datetime | None:
    """Count one failure; for credential failures over the limit, ban the IP. Returns a new ban's end."""
    await ensure_schema()
    async with get_session() as session:
        session.add(AuthFailure(ip=ip[:64], kind=kind))
        await session.execute(delete(AuthFailure).where(cast(Any, AuthFailure.ts) < _now() - _KEEP_FAILURES))
        await session.commit()
    if kind != "credential" or await _recent_failures(ip, "credential", CREDENTIAL_WINDOW) < CREDENTIAL_LIMIT:
        return None
    if await banned_until(ip) is not None:
        return None
    until = _now() + BAN_DURATION
    async with get_session() as session:
        row = await session.get(IpBan, ip) or IpBan(ip=ip, until_ts=until)
        row.until_ts = until
        row.reason = f"{CREDENTIAL_LIMIT}+ rejected credentials in {int(CREDENTIAL_WINDOW.total_seconds() // 60)} min"
        row.created_ts = _now()
        session.add(row)
        await session.commit()
    _ban_cache[ip] = (until, time.monotonic())
    return until


async def list_bans() -> list[IpBan]:
    await ensure_schema()
    async with get_session() as session:
        rows = (await session.execute(select(IpBan).where(cast(Any, IpBan.until_ts) > _now()))).scalars().all()
    return sorted(rows, key=lambda r: r.until_ts)


async def clear(ip: str) -> bool:
    """Lift a ban and forget the IP's recent failures (also unpauses its sign-ins)."""
    await ensure_schema()
    async with get_session() as session:
        row = await session.get(IpBan, ip)
        if row is not None:
            await session.delete(row)
        await session.execute(delete(AuthFailure).where(cast(Any, AuthFailure.ip == ip)))
        await session.commit()
    _ban_cache.pop(ip, None)
    return row is not None
