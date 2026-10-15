"""HTTP transport helpers wrapping FastMCP with FastAPI."""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import hashlib
import hmac
import importlib
import json
import logging
import re
import secrets
import time
from collections.abc import MutableMapping
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, urlencode

import httpx
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.exception_handlers import http_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import Receive, Scope, Send

from . import audit, ip_guard, oidc, ui_auth
from .app import (
    CLIENT_SCRIPTS_DIR,
    AttachmentTooLargeError,
    ToolExecutionError,
    _blob_path,
    _expire_stale_file_reservations,
    _format_cross_project_agent_address,
    _get_agent,
    _get_file_attachment,
    _get_project_by_identifier,
    _message_sent_event_payload,
    _presence_listing,
    _record_event,
    _record_presence,
    _sender_display_name,
    _store_file_attachment,
    _tool_metrics_snapshot,
    build_mcp_server,
    client_version_tuple,
    get_project_sibling_data,
    refresh_project_sibling_suggestions,
    served_client_version,
    sweep_presence,
    sweep_stale_agents,
    update_project_sibling_status,
)
from .config import Settings, get_settings
from .db import ensure_schema, get_session
from .models import Event
from .storage import (
    ProjectArchive,
    archive_write_lock,
    collect_lock_status,
    ensure_archive,
    get_agent_communication_graph,
    get_archive_tree,
    get_commit_detail,
    get_fd_headroom,
    get_fd_usage,
    get_file_content,
    get_historical_inbox_snapshot,
    get_lock_telemetry,
    get_message_commit_sha,
    get_recent_commits,
    get_repo_cache_stats,
    get_timeline_commits,
    proactive_fd_cleanup,
    write_agent_profile,
    write_file_reservation_record,
)


async def _project_slug_from_id(pid: int | None) -> str | None:
    if pid is None:
        return None
    async with get_session() as session:
        row = await session.execute(text("SELECT slug FROM projects WHERE id = :pid"), {"pid": pid})
        res = row.fetchone()
        return res[0] if res and res[0] else None


async def _ensure_ack_escalation_holder(
    *,
    settings: Settings,
    project_id: int,
    project_slug: str | None,
    recipient_agent_id: int,
    recipient_name: str,
    claim_name: str,
    now: datetime,
    now_naive: datetime,
) -> tuple[int, str]:
    """Return the holder identity for ACK escalation, creating the ops holder if needed.

    When a synthetic holder must be created, the DB insert happens first and the
    archive profile write follows only after the session has closed. This keeps
    the ACK worker out of the DB->archive lock ordering that can deadlock mixed
    HTTP and MCP traffic.
    """
    holder_agent_id = int(recipient_agent_id)
    holder_agent_name = recipient_name
    holder_profile_payload: dict[str, Any] | None = None

    async with get_session() as s_holder:
        hid_row = await s_holder.execute(
            text("SELECT id FROM agents WHERE project_id = :pid AND name = :name"),
            {"pid": project_id, "name": claim_name},
        )
        hid = hid_row.scalar_one_or_none()
        if isinstance(hid, int):
            return hid, claim_name

        await s_holder.execute(
            text(
                "INSERT OR IGNORE INTO agents(project_id, name, program, model, task_description, inception_ts, last_active_ts, attachments_policy, contact_policy) VALUES (:pid, :name, :program, :model, :task, :ts, :ts, :attachments_policy, :contact_policy)"
            ),
            {
                "pid": project_id,
                "name": claim_name,
                "program": "ops",
                "model": "system",
                "task": "ops-escalation",
                "ts": now_naive,
                "attachments_policy": "auto",
                "contact_policy": "auto",
            },
        )
        await s_holder.commit()
        hid_row2 = await s_holder.execute(
            text("SELECT id FROM agents WHERE project_id = :pid AND name = :name"),
            {"pid": project_id, "name": claim_name},
        )
        hid2 = hid_row2.scalar_one_or_none()
        if isinstance(hid2, int):
            holder_agent_id = hid2
            holder_agent_name = claim_name
            if project_slug:
                holder_profile_payload = {
                    "id": holder_agent_id,
                    "name": holder_agent_name,
                    "program": "ops",
                    "model": "system",
                    "task_description": "ops-escalation",
                    "inception_ts": now.isoformat(),
                    "last_active_ts": now.isoformat(),
                    "project_id": project_id,
                    "attachments_policy": "auto",
                    "contact_policy": "auto",
                }

    if holder_profile_payload is not None and project_slug:
        archive = await ensure_archive(settings, project_slug)
        async with archive_write_lock(archive):
            await write_agent_profile(archive, holder_profile_payload)

    return holder_agent_id, holder_agent_name


def _http_sender_identity(
    *,
    message_project_id: int | None,
    sender_name: str | None,
    sender_project_id: int | None,
    sender_project_human_key: str | None,
    sender_project_slug: str | None,
) -> tuple[str, dict[str, str]]:
    canonical_sender = (sender_name or "").strip() or "Unknown"
    sender_display = _sender_display_name(
        message_project_id=message_project_id,
        sender_name=canonical_sender,
        sender_project_id=sender_project_id,
        sender_project_slug=sender_project_slug,
    )
    metadata: dict[str, str] = {"sender_name": canonical_sender}
    if (
        message_project_id is None
        or sender_project_id is None
        or sender_project_id == message_project_id
    ):
        return sender_display, metadata
    if sender_project_human_key:
        metadata["sender_project"] = sender_project_human_key
    if sender_project_slug:
        metadata["sender_project_slug"] = sender_project_slug
        metadata["sender_address"] = _format_cross_project_agent_address(
            sender_project_slug,
            canonical_sender,
        )
    return sender_display, metadata


_HTTP_MESSAGE_SUBJECT_SLUG_RE = re.compile(r"[^a-zA-Z0-9]+")


def _coerce_http_archive_timestamp(created_ts_raw: Any) -> datetime:
    try:
        if isinstance(created_ts_raw, str):
            text_value = (
                created_ts_raw.replace("Z", "+00:00")
                if created_ts_raw.endswith("Z")
                else created_ts_raw
            )
            dt = datetime.fromisoformat(text_value)
        else:
            dt = created_ts_raw
        if not isinstance(dt, datetime):
            raise TypeError("created timestamp must be a datetime")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _build_http_archive_message_filename(created_ts_raw: Any, subject_raw: str, message_id: int) -> tuple[str, str, str]:
    dt = _coerce_http_archive_timestamp(created_ts_raw)
    y_dir = dt.strftime("%Y")
    m_dir = dt.strftime("%m")
    created_iso = dt.strftime("%Y-%m-%dT%H-%M-%SZ")
    subject_slug = (
        _HTTP_MESSAGE_SUBJECT_SLUG_RE.sub("-", subject_raw).strip("-_").lower()[:80]
        or "message"
    )
    return y_dir, m_dir, f"{created_iso}__{subject_slug}__{message_id}.md"


async def _delete_messages_from_archive(
    *,
    settings: Settings,
    project_slug: str,
    messages_to_delete: list[tuple[Any, ...]],
    recip_map: dict[int, list[str]],
    commit_message: str,
) -> int:
    archive = await ensure_archive(settings, project_slug)
    git_paths_removed: list[str] = []
    seen_git_paths: set[str] = set()

    async with archive_write_lock(archive):
        for mrow in messages_to_delete:
            msg_id = int(mrow[0])
            y_dir, m_dir, filename = _build_http_archive_message_filename(
                mrow[1],
                str(mrow[2] or ""),
                msg_id,
            )
            sender_name = str(mrow[3] or "")

            candidate_dirs = [
                archive.root / "messages" / y_dir / m_dir,
                archive.root / "agents" / sender_name / "outbox" / y_dir / m_dir,
            ]
            for recip_name in recip_map.get(msg_id, []):
                candidate_dirs.append(
                    archive.root / "agents" / recip_name / "inbox" / y_dir / m_dir
                )

            for cdir in candidate_dirs:
                fpath = cdir / filename
                rel = fpath.relative_to(archive.repo_root).as_posix()
                try:
                    await asyncio.to_thread(fpath.unlink)
                except FileNotFoundError:
                    continue
                except OSError:
                    continue
                if rel not in seen_git_paths:
                    seen_git_paths.add(rel)
                    git_paths_removed.append(rel)

        if git_paths_removed:
            actor_module = importlib.import_module("git")
            actor_cls = actor_module.Actor
            git_actor = actor_cls(
                settings.storage.git_author_name,
                settings.storage.git_author_email,
            )
            await asyncio.to_thread(
                archive.repo.index.remove,
                git_paths_removed,
                working_tree=False,
            )
            await asyncio.to_thread(
                archive.repo.index.commit,
                commit_message,
                author=git_actor,
                committer=git_actor,
            )

    return len(git_paths_removed)


__all__ = ["build_http_app", "create_app", "main"]


class _FastMCPHttpApp(Protocol):
    def http_app(self, *args: Any, **kwargs: Any) -> FastAPI: ...


class _FastAPILifespan(Protocol):
    def lifespan(self, app: FastAPI) -> Any: ...


def _expanduser_resolve_path(path: Path) -> Path:
    return path.expanduser().resolve()


def _path_exists(path: Path) -> bool:
    return path.exists()


def _open_git_repo(repo_root: Path):
    from git import Repo as GitRepo

    return GitRepo(str(repo_root))


async def _open_existing_project_archive(settings: Settings, slug: str) -> ProjectArchive | None:
    """Open an existing project archive for read-only routes without creating new directories."""
    repo_root = await asyncio.to_thread(_expanduser_resolve_path, Path(settings.storage.root))
    if not await asyncio.to_thread(_path_exists, repo_root / ".git"):
        return None
    project_root = repo_root / "projects" / slug
    if not await asyncio.to_thread(_path_exists, project_root):
        return None
    repo = await asyncio.to_thread(_open_git_repo, repo_root)
    return ProjectArchive(
        settings=settings,
        slug=slug,
        root=project_root,
        repo=repo,
        lock_path=project_root / ".archive.lock",
        repo_root=repo_root,
    )


def _collect_retention_quota_report_sync(settings: Settings) -> dict[str, Any]:
    import datetime as _dt
    import fnmatch as _fnmatch

    storage_root = _expanduser_resolve_path(Path(settings.storage.root))
    projects_root = storage_root / "projects"
    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(
        days=int(settings.retention_max_age_days)
    )
    old_messages = 0
    total_attach_bytes = 0
    per_project_attach: dict[str, int] = {}
    per_project_inbox_counts: dict[str, int] = {}
    ignore_patterns = list(getattr(settings, "retention_ignore_project_patterns", []) or [])

    for proj_dir in projects_root.iterdir() if projects_root.exists() else []:
        if not proj_dir.is_dir():
            continue
        proj_name = proj_dir.name
        if any(_fnmatch.fnmatch(proj_name, pat) for pat in ignore_patterns):
            continue
        msg_root = proj_dir / "messages"
        if msg_root.exists():
            for ydir in msg_root.iterdir():
                for mdir in ydir.iterdir() if ydir.is_dir() else []:
                    for file_path in mdir.iterdir() if mdir.is_dir() else []:
                        if file_path.suffix.lower() != ".md":
                            continue
                        with contextlib.suppress(Exception):
                            ts = _dt.datetime.fromtimestamp(file_path.stat().st_mtime, _dt.timezone.utc)
                            if ts < cutoff:
                                old_messages += 1
        inbox_root = proj_dir / "agents"
        if inbox_root.exists():
            count_inbox = 0
            for inbox_file in inbox_root.rglob("inbox/*/*/*.md"):
                with contextlib.suppress(Exception):
                    if inbox_file.is_file():
                        count_inbox += 1
            per_project_inbox_counts[proj_name] = count_inbox
        att_root = proj_dir / "attachments"
        if att_root.exists():
            for attachment_file in att_root.rglob("*.webp"):
                with contextlib.suppress(Exception):
                    size_bytes = attachment_file.stat().st_size
                    total_attach_bytes += size_bytes
                    per_project_attach[proj_name] = per_project_attach.get(proj_name, 0) + size_bytes

    return {
        "old_messages": old_messages,
        "retention_max_age_days": int(settings.retention_max_age_days),
        "total_attachments_bytes": total_attach_bytes,
        "quota_limit_bytes": int(settings.quota_attachments_limit_bytes),
        "per_project_attach": per_project_attach,
        "per_project_inbox_counts": per_project_inbox_counts,
    }


async def _collect_retention_quota_report(settings: Settings) -> dict[str, Any]:
    return await asyncio.to_thread(_collect_retention_quota_report_sync, settings)


def _collect_archive_guide_stats_sync(settings: Settings) -> dict[str, Any]:
    import subprocess as _subprocess
    from itertools import islice

    storage_root = str(_expanduser_resolve_path(Path(settings.storage.root)))
    repo_root = Path(storage_root)
    total_commits = "0"
    project_count = 0
    repo_size = "0 MB"
    last_commit_time = "Never"

    if _path_exists(repo_root / ".git"):
        repo = None
        try:
            repo = _open_git_repo(repo_root)
            commit_count = sum(1 for _ in repo.iter_commits(max_count=10000))
            total_commits = "10,000+" if commit_count == 10000 else f"{commit_count:,}"
            last_commit = next(repo.iter_commits(max_count=1), None)
            last_commit_time = last_commit.authored_datetime.strftime("%b %d, %Y") if last_commit else "Never"

            projects_dir = repo_root / "projects"
            if projects_dir.exists():
                project_count = sum(1 for p in islice(projects_dir.iterdir(), 100) if p.is_dir())

            try:
                result = _subprocess.run(
                    ["du", "-sh", str(repo_root)],
                    capture_output=True,
                    text=True,
                    timeout=5.0,
                )
                repo_size = result.stdout.split()[0] if getattr(result, "returncode", 1) == 0 else "Unknown"
            except (_subprocess.TimeoutExpired, FileNotFoundError, PermissionError, OSError):
                repo_size = "Unknown"
        except Exception:
            pass
        finally:
            if repo is not None:
                repo.close()

    return {
        "storage_root": storage_root,
        "total_commits": total_commits,
        "project_count": project_count,
        "repo_size": repo_size,
        "last_commit_time": last_commit_time,
    }


def _decode_jwt_header_segment(token: str) -> dict[str, object] | None:
    """Return decoded JWT header without verifying signature."""
    try:
        segment = token.split(".", 1)[0]
        padded = segment + "=" * (-len(segment) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


_LOGGING_CONFIGURED = False

# Pre-compiled regex patterns for HTTP validators
_SLUG_VALIDATOR_RE = re.compile(r"^[a-z0-9_-]+$", re.IGNORECASE)
_AGENT_NAME_VALIDATOR_RE = re.compile(r"^[A-Za-z0-9]+$")
_TIMESTAMP_VALIDATOR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")

_LIKE_ESCAPE_CHAR = "!"


def _like_escape(term: str) -> str:
    """Escape LIKE wildcards for literal substring matching."""
    return term.replace("!", "!!").replace("%", "!%").replace("_", "!_")


def _configure_logging(settings: Settings) -> None:
    """Initialize structlog and stdlib logging formatting."""
    # Idempotent setup
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
    ]
    if settings.log_json_enabled:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.processors.KeyValueRenderer(key_order=["event", "path", "status"]))
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, settings.log_level.upper(), logging.INFO)),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))

    # Suppress verbose MCP library logging for stateless HTTP sessions
    # "Terminating session: None" is routine for stateless mode and just noise
    logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
    logging.getLogger("mcp.server.lowlevel.server").setLevel(logging.WARNING)

    # Suppress verbose aiosqlite DEBUG logs (functools.partial cursor/operation noise)
    logging.getLogger("aiosqlite").setLevel(logging.INFO)

    # Suppress verbose git library DEBUG logs (Popen commands, platform detection)
    logging.getLogger("git.util").setLevel(logging.INFO)
    logging.getLogger("git.cmd").setLevel(logging.INFO)

    # Suppress filelock DEBUG logs (lock acquire/release routine operations)
    logging.getLogger("filelock").setLevel(logging.INFO)

    # Suppress SSE ping keepalive debug logs (periodic noise every 15s)
    logging.getLogger("sse_starlette.sse").setLevel(logging.INFO)

    # Add filter to suppress verbose tracebacks for expected/recoverable errors
    # FastMCP's tool_manager uses logger.exception() which prints full tracebacks
    # even for expected errors like "agent not found" or "git lock contention".
    # This filter intercepts those and removes the traceback for cleaner logs.
    class ExpectedErrorFilter(logging.Filter):
        """Filter that suppresses tracebacks for expected/recoverable tool errors.

        Expected errors include:
        - ToolExecutionError with recoverable=True
        - Agent not found / project not found
        - Git index.lock contention
        - Resource busy / database lock

        These are normal operational conditions in multi-agent environments
        and don't need full stack traces cluttering the logs.
        """

        # Keywords that indicate an expected/recoverable error
        _EXPECTED_PATTERNS = (
            "not found in project",
            "index.lock",
            "git_index_lock",
            "resource_busy",
            "temporarily locked",
            "recoverable=true",
            "use register_agent",
            "available agents:",
        )

        def filter(self, record: logging.LogRecord) -> bool:
            # Only process records from FastMCP tool_manager with exception info
            if not record.exc_info or record.exc_info[1] is None:
                return True

            exc = record.exc_info[1]
            exc_str = str(exc).lower()

            # Check if this is an expected error based on message content
            is_expected = any(pattern in exc_str for pattern in self._EXPECTED_PATTERNS)

            # Also check for our ToolExecutionError with recoverable flag
            if hasattr(exc, "recoverable") and exc.recoverable:
                is_expected = True

            # Check the cause chain for ToolExecutionError
            cause = getattr(exc, "__cause__", None)
            if cause is not None:
                cause_str = str(cause).lower()
                if any(pattern in cause_str for pattern in self._EXPECTED_PATTERNS):
                    is_expected = True
                if hasattr(cause, "recoverable") and cause.recoverable:
                    is_expected = True

            if is_expected:
                # Clear exc_info to prevent traceback printing, but keep the log message
                record.exc_info = None
                record.exc_text = None
                # Downgrade from ERROR to INFO for expected errors
                if record.levelno >= logging.ERROR:
                    record.levelno = logging.INFO
                    record.levelname = "INFO"

            return True

    # Apply filter to FastMCP's tool_manager logger
    fastmcp_logger = logging.getLogger("fastmcp.tools.tool_manager")
    fastmcp_logger.addFilter(ExpectedErrorFilter())

    # mark configured
    _LOGGING_CONFIGURED = True


# In-process JWKS cache: avoid refetching the JWKS document on every request
# (#212). Keyed by JWKS URL; entries expire after _JWKS_CACHE_TTL_SECONDS.
_JWKS_CACHE_TTL_SECONDS = 300.0
_jwks_cache: dict[str, tuple[float, Any]] = {}
_jwks_cache_lock = asyncio.Lock()


async def _fetch_jwks(jwks_url: str, *, force: bool = False):
    """Return a parsed JWKS key set for ``jwks_url``, using a TTL cache.

    On a cache miss/expiry (or when ``force`` is set, e.g. after an unknown
    ``kid``), the document is refetched. On fetch/parse failure the last good
    cached key set (if any) is returned so transient outages don't break auth.
    """
    from time import monotonic

    jose_mod = importlib.import_module("authlib.jose")
    JsonWebKey = jose_mod.JsonWebKey

    now = monotonic()
    async with _jwks_cache_lock:
        cached = _jwks_cache.get(jwks_url)
        if cached is not None and not force and (now - cached[0]) < _JWKS_CACHE_TTL_SECONDS:
            return cached[1]

    try:
        httpx = importlib.import_module("httpx")
        AsyncClient = httpx.AsyncClient
        async with AsyncClient(timeout=5) as client:
            jwks = (await client.get(jwks_url)).json()
        key_set = JsonWebKey.import_key_set(jwks)
    except Exception:
        # Fall back to any cached (possibly stale) key set on fetch failure.
        async with _jwks_cache_lock:
            cached = _jwks_cache.get(jwks_url)
        return cached[1] if cached is not None else None

    async with _jwks_cache_lock:
        _jwks_cache[jwks_url] = (monotonic(), key_set)
    return key_set


def _select_jwks_key(key_set, header: dict, algorithms: list[str]):
    """Resolve the verification key from a JWKS key set by ``kid``.

    Never blindly picks ``keys[0]`` (#211). With a ``kid`` we look it up
    directly; an unknown ``kid`` returns ``None``. Without a ``kid`` this also
    returns ``None`` -- the caller falls back to verifying against each
    algorithm-compatible candidate (see ``_jwks_candidate_keys``) instead.
    """
    kid = header.get("kid")
    if kid:
        with contextlib.suppress(Exception):
            return key_set.find_by_kid(kid)
    return None


def _jwks_candidate_keys(key_set, header: dict, algorithms: list[str]) -> list:
    """Return JWKS keys to try when no ``kid`` is present.

    Filters by signing use and by algorithm compatibility (matching the key's
    declared ``alg`` when present, otherwise the key type implied by the
    configured algorithms). Blind ``keys[0]`` selection is never used.
    """
    alg_set = {str(a) for a in algorithms}
    # Map configured JWS algorithms to acceptable JWK key types.
    kty_for_alg = {
        "HS": "oct", "RS": "RSA", "PS": "RSA",
        "ES": "EC", "Ed": "OKP",
    }
    wanted_kty = {kty_for_alg[a[:2]] for a in alg_set if a[:2] in kty_for_alg}
    candidates = []
    for key in list(getattr(key_set, "keys", []) or []):
        with contextlib.suppress(Exception):
            use = key.tokens.get("use") if hasattr(key, "tokens") else None
            if use not in (None, "sig"):
                continue
            key_alg = key.tokens.get("alg") if hasattr(key, "tokens") else None
            if key_alg is not None and str(key_alg) not in alg_set:
                continue
            kty = getattr(key, "kty", None) or (key.tokens.get("kty") if hasattr(key, "tokens") else None)
            if wanted_kty and kty is not None and kty not in wanted_kty:
                continue
            candidates.append(key)
    return candidates


class BearerAuthMiddleware(BaseHTTPMiddleware):
    def __init__(
        self, app: FastAPI, token: str, allow_localhost: bool = False, jwt_enabled: bool = False
    ) -> None:
        super().__init__(app)
        self._token = token
        self._allow_localhost = allow_localhost
        # When JWT auth is also enabled, a static-bearer mismatch must NOT
        # short-circuit before the inner SecurityAndRateLimitMiddleware gets a
        # chance to validate a JWT (#210). In that case we accept any Bearer
        # token here and let the JWT path render the final auth decision.
        self._jwt_enabled = jwt_enabled

    @staticmethod
    def _is_localhost(host: str) -> bool:
        """Check if host is a localhost address, including IPv4-mapped IPv6."""
        if not host:
            return False
        # Standard localhost addresses
        if host in {"127.0.0.1", "::1", "localhost"}:
            return True
        # IPv4-mapped IPv6 address (::ffff:127.0.0.1)
        return bool(host.lower().startswith("::ffff:") and host[7:] == "127.0.0.1")

    @staticmethod
    def _has_forwarded_headers(request: Request) -> bool:
        """Detect proxy-forwarded headers to avoid trusting localhost behind proxies."""
        headers = request.headers
        return any(
            name in headers
            for name in ("x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "forwarded")
        )

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        if request.method == "OPTIONS":  # allow CORS preflight
            return await call_next(request)
        if request.url.path.startswith("/health/") or request.url.path == "/api/health":
            return await call_next(request)
        # Per-IP guard (ip_guard.py): banned addresses get nothing; every 401 counts towards a ban.
        # (Public files below come after it, so a banned scanner can't fetch the installer either.)
        ip = ip_guard.client_ip(request)
        if ip is not None and (until := await ip_guard.banned_until(ip)) is not None:
            retry = max(1, int((until - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()))
            return JSONResponse(
                {"detail": "Too many failed attempts from this address. Try again later."},
                status_code=status.HTTP_403_FORBIDDEN,
                headers={"Retry-After": str(retry)},
            )
        response = await self._authenticate(request, call_next)
        if ip is not None and response.status_code == status.HTTP_401_UNAUTHORIZED:
            new_ban = await ip_guard.record_failure(ip, "credential")
            if new_ban is not None:
                await audit.record(
                    "ip.banned", actor="server", target_type="ip", target_id=ip,
                    details=f"until={new_ban.isoformat(timespec='seconds')}Z; {ip_guard.CREDENTIAL_LIMIT}+ rejected credentials",
                    request=request,
                )
        return response

    async def _authenticate(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path in PUBLIC_CLIENT_PATHS:
            return await call_next(request)
        if _localhost_bypass_allowed(
            request,
            allow_localhost=self._allow_localhost,
        ):
            return await call_next(request)
        if await _ui_request_authenticated(request):
            return await call_next(request)
        auth_header = request.headers.get("Authorization", "")
        expected_header = f"Bearer {self._token}"
        # Use constant-time comparison to prevent timing attacks
        if hmac.compare_digest(auth_header, expected_header):
            return await call_next(request)
        # A person's own token (mail_wake.py login), accepted wherever the shared one is.
        if auth_header.startswith(f"Bearer {ui_auth.TOKEN_PREFIX}"):
            owner = await ui_auth.api_token_owner(auth_header[len("Bearer ") :].strip())
            if owner is not None:
                request.state.agent_mail_owner = owner
                return await call_next(request)
        # Static bearer did not match. If JWT auth is enabled, defer to the inner
        # JWT-validating middleware instead of rejecting here, so EITHER a valid
        # static bearer OR a valid JWT is accepted (#210).
        if self._jwt_enabled and auth_header.startswith("Bearer "):
            return await call_next(request)
        path = request.url.path
        if _is_mail_ui_path(path) and request.method == "GET" and "text/html" in request.headers.get("accept", ""):
            # A person opening a page: send them to the login form instead of a bare JSON 401.
            target = path + (f"?{request.url.query}" if request.url.query else "")
            return RedirectResponse(f"{UI_LOGIN_PATH}?{urlencode({'next': target})}", status_code=status.HTTP_303_SEE_OTHER)
        return JSONResponse({"detail": "Unauthorized"}, status_code=status.HTTP_401_UNAUTHORIZED)


# Served without authentication, because a machine running the installer has no token yet:
# the installer and the two client scripts it downloads. Exactly these paths and nothing else
# (no directory listing, no other files). The scripts are this repo's open-source code.
CLIENT_SCRIPTS = ("mail_wake.py", "agent_mail_files.py")
PUBLIC_CLIENT_PATHS = frozenset({"/install.sh", *(f"/client/{name}" for name in CLIENT_SCRIPTS)})
INSTALLER_PLACEHOLDER = "__AGENT_MAIL_SERVER__"
# The client scripts send their CLIENT_VERSION in this header. Scripts from before versioning
# didn't, but they are recognisable by their User-Agent, and count as version "".
CLIENT_VERSION_HEADER = "X-Agent-Mail-Client"
LEGACY_CLIENT_USER_AGENT = "mcp_agent_mail-wake"


def _client_script_version(request: Request) -> str | None:
    """The calling client script's version ("" for pre-versioning scripts), or None for anything else."""
    if (version := request.headers.get(CLIENT_VERSION_HEADER)) is not None:
        return version.strip()
    if request.headers.get("user-agent", "").startswith(LEGACY_CLIENT_USER_AGENT):
        return ""
    return None


class ClientVersionMiddleware(BaseHTTPMiddleware):
    """Refuse client scripts older than CLIENT_MIN_VERSION with 426 and a message their hooks show the agent.

    Only the API the scripts use (/mail/api/) is gated; /client/ and /install.sh stay open so they can update.
    """

    def __init__(self, app: Any, minimum: str) -> None:
        super().__init__(app)
        self._minimum = minimum

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        version = _client_script_version(request)
        if (
            version is not None
            and request.url.path.startswith("/mail/api/")
            and client_version_tuple(version) < client_version_tuple(self._minimum)
        ):
            return JSONResponse(
                {
                    "detail": (
                        f"This machine's Agent Mail client ({version or 'unversioned'}) is older than the server's "
                        f"minimum ({self._minimum}), so the server refuses it."
                    ),
                    "minimum": self._minimum,
                    "latest": served_client_version(),
                },
                status_code=status.HTTP_426_UPGRADE_REQUIRED,
            )
        return await call_next(request)


# Browser login for the /mail UI. Browsers cannot attach an Authorization
# header to page loads or EventSource connections, so people sign in at
# /mail/login (GitHub, password + MFA, or the bearer token as an admin
# fallback) and receive this cookie instead. It holds a random session token;
# the session lives on the server (ui_auth.py), so it can be revoked, and each
# request re-checks that its user is still allowed in.
UI_SESSION_COOKIE = "agent_mail_ui"
UI_SESSION_MAX_AGE_SECONDS = int(ui_auth.SESSION_MAX_AGE.total_seconds())
UI_LOGIN_PATH = "/mail/login"
UI_AUTH_PREFIX = "/mail/auth/"  # sign-in flows (GitHub OAuth), reachable while signed out
UI_OAUTH_STATE_COOKIE = "agent_mail_oauth"
UI_OAUTH_STATE_MAX_AGE_SECONDS = 600
UI_MFA_COOKIE = "agent_mail_mfa"  # password passed, second factor still to come
MFA_PENDING_MAX_AGE_SECONDS = 300
# The session user for a bearer-token sign-in. GitHub logins can't contain "_",
# so it never collides with a real user, and it needs no cookie quoting.
TOKEN_LOGIN_USER = "_token"
OIDC_SESSION_PREFIX = "sso:"  # company sign-in sessions: sso:<verified email>
UI_OIDC_STATE_COOKIE = "agent_mail_oidc"


def _is_mail_ui_path(path: str) -> bool:
    return path == "/mail" or path.startswith("/mail/")


def _ui_sign(token: str, payload: str) -> str:
    """HMAC for the short-lived sign-in cookies (OAuth state, MFA pending), keyed by the bearer token."""
    return hmac.new(token.encode(), f"mail-ui:{payload}".encode(), hashlib.sha256).hexdigest()


def _oidc_enabled() -> bool:
    s = get_settings().http
    return bool(s.oidc_issuer and s.oidc_client_id and s.oidc_client_secret)


def _oidc_email_allowed(email: str) -> bool:
    s = get_settings().http
    email = email.lower()
    domain = email.rpartition("@")[2]
    return email in s.oidc_allowed_emails or (bool(domain) and domain in s.oidc_allowed_domains)


async def _ui_user_allowed(user: str) -> bool:
    """Whether a session's user may still be signed in under the server's current settings."""
    http_settings = get_settings().http
    if user.startswith(OIDC_SESSION_PREFIX):
        return _oidc_enabled() and _oidc_email_allowed(user.removeprefix(OIDC_SESSION_PREFIX))
    if http_settings.ui_sso_only:
        return False  # SSO-only: every other kind of session ends
    if user == TOKEN_LOGIN_USER:
        return bool(http_settings.bearer_token) and http_settings.ui_token_login
    if user.startswith(ui_auth.SESSION_PREFIX):
        return await ui_auth.local_account_active(user.removeprefix(ui_auth.SESSION_PREFIX))
    github_on = bool(http_settings.github_client_id and http_settings.github_client_secret)
    return github_on and user.lower() in http_settings.github_allowed_users


async def _ui_session(request: Request) -> Any:
    """The request's live session (a ui_sessions row) if its user is still allowed in, else None.

    Background traffic (the Live page's polling and event stream, under /mail/api/)
    doesn't count as activity, so an open tab can't keep a session alive forever.
    """
    if not get_settings().http.bearer_token:
        return None
    row = await ui_auth.lookup_session(
        request.cookies.get(UI_SESSION_COOKIE, ""), activity=not request.url.path.startswith("/mail/api/")
    )
    if row is None or not await _ui_user_allowed(row.user):
        return None
    return row


async def _ui_session_user(request: Request) -> str | None:
    row = await _ui_session(request)
    return row.user if row is not None else None


def _github_http_client() -> httpx.AsyncClient:
    """HTTP client for GitHub's OAuth and API endpoints (tests replace it with a fake GitHub)."""
    return httpx.AsyncClient(
        timeout=10.0,
        headers={"User-Agent": "mcp_agent_mail (+https://github.com/Dicklesworthstone/mcp_agent_mail)"},
    )


async def _github_login_for_code(code: str, client_id: str, client_secret: str, redirect_uri: str) -> str:
    """Exchange an OAuth callback code for the signed-in user's GitHub login."""
    async with _github_http_client() as client:
        token_response = await client.post(
            "https://github.com/login/oauth/access_token",
            data={"client_id": client_id, "client_secret": client_secret, "code": code, "redirect_uri": redirect_uri},
            headers={"Accept": "application/json"},
        )
        token_response.raise_for_status()
        payload = token_response.json()
        if "access_token" not in payload:
            raise ValueError(f"GitHub returned no access token: {payload.get('error', 'unknown error')}")
        user_response = await client.get(
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {payload['access_token']}", "Accept": "application/vnd.github+json"},
        )
        user_response.raise_for_status()
        return str(user_response.json()["login"])


async def _ui_request_authenticated(request: Request) -> bool:
    """/mail requests that need no Authorization header: sign-in pages, or a live session."""
    path = request.url.path
    if not _is_mail_ui_path(path):
        return False
    return path == UI_LOGIN_PATH or path.startswith(UI_AUTH_PREFIX) or await _ui_session_user(request) is not None


def _localhost_bypass_allowed(request: Request, *, allow_localhost: bool) -> bool:
    """Return whether this request qualifies for localhost auth bypass."""
    if not allow_localhost:
        return False
    try:
        client_host = request.client.host if request.client else ""
    except Exception:
        client_host = ""
    return BearerAuthMiddleware._is_localhost(client_host) and not BearerAuthMiddleware._has_forwarded_headers(
        request
    )


class SecurityAndRateLimitMiddleware(BaseHTTPMiddleware):
    """JWT auth (optional), RBAC, and token-bucket rate limiting.

    - If JWT is enabled, validates Authorization: Bearer <token> using either HMAC secret or JWKS URL.
    - Enforces basic RBAC when enabled: read-only roles may only call whitelisted tools and resource reads.
    - Applies per-endpoint token-bucket limits (tools vs resources) with in-memory or Redis backend.
    """

    def __init__(self, app: FastAPI, settings: Settings):
        super().__init__(app)
        self.settings = settings
        self._jwt_enabled = bool(getattr(settings.http, "jwt_enabled", False))
        self._rbac_enabled = bool(getattr(settings.http, "rbac_enabled", True))
        self._reader_roles = set(getattr(settings.http, "rbac_reader_roles", []) or [])
        self._writer_roles = set(getattr(settings.http, "rbac_writer_roles", []) or [])
        self._readonly_tools = set(getattr(settings.http, "rbac_readonly_tools", []) or [])
        self._default_role = getattr(settings.http, "rbac_default_role", "tools")
        # Token bucket state (memory)
        from time import monotonic

        self._monotonic = monotonic
        self._buckets: dict[str, tuple[float, float]] = {}
        self._last_cleanup = monotonic()
        # Redis client (optional)
        self._redis = None
        if getattr(settings.http, "rate_limit_backend", "memory") == "redis" and getattr(
            settings.http, "rate_limit_redis_url", ""
        ):
            try:
                redis_asyncio = importlib.import_module("redis.asyncio")
                Redis = redis_asyncio.Redis
                self._redis = Redis.from_url(settings.http.rate_limit_redis_url)
            except Exception:
                self._redis = None

    def _cleanup_buckets(self, now: float) -> None:
        """Remove stale buckets to prevent memory leaks."""
        # Evict buckets not accessed in the last hour
        expiration = 3600.0
        cutoff = now - expiration
        # Create list of keys to remove to avoid runtime modification errors during iteration
        to_remove = [k for k, (_, ts) in self._buckets.items() if ts < cutoff]
        for k in to_remove:
            self._buckets.pop(k, None)

    async def _decode_jwt(self, token: str) -> dict | None:
        """Validate and decode JWT, returning claims or None on failure."""
        with contextlib.suppress(Exception):
            jose_mod = importlib.import_module("authlib.jose")
            JsonWebKey = jose_mod.JsonWebKey
            JsonWebToken = jose_mod.JsonWebToken
            algs = list(getattr(self.settings.http, "jwt_algorithms", ["HS256"]))
            jwt = JsonWebToken(algs)
            audience = getattr(self.settings.http, "jwt_audience", None) or None
            issuer = getattr(self.settings.http, "jwt_issuer", None) or None
            jwks_url = getattr(self.settings.http, "jwt_jwks_url", None) or None
            secret = getattr(self.settings.http, "jwt_secret", None) or None

            header = _decode_jwt_header_segment(token)
            if header is None:
                return None
            key = None
            candidate_keys: list = []
            if jwks_url:
                with contextlib.suppress(Exception):
                    key_set = await _fetch_jwks(jwks_url)
                    if key_set is None:
                        return None
                    if header.get("kid"):
                        key = _select_jwks_key(key_set, header, algs)
                        # Unknown kid: the cached JWKS may be stale; force a
                        # refresh once before giving up (#212).
                        if key is None:
                            key_set = await _fetch_jwks(jwks_url, force=True)
                            if key_set is not None:
                                key = _select_jwks_key(key_set, header, algs)
                    else:
                        # No kid: never blind-pick keys[0]. Try every
                        # algorithm-compatible key during verification (#211).
                        candidate_keys = _jwks_candidate_keys(key_set, header, algs)
            elif secret:
                with contextlib.suppress(Exception):
                    key = JsonWebKey.import_key(secret, {"kty": "oct"})
            keys_to_try = candidate_keys if candidate_keys else ([key] if key is not None else [])
            if not keys_to_try:
                return None
            for candidate in keys_to_try:
                with contextlib.suppress(Exception):
                    claims = jwt.decode(token, candidate)
                    if audience:
                        claims.validate_aud(audience)
                    if issuer and str(claims.get("iss") or "") != issuer:
                        continue
                    claims.validate()
                    return dict(claims)
        return None

    @staticmethod
    def _classify_request(path: str, method: str, body_bytes: bytes) -> tuple[str, str | None]:
        """Return (kind, tool_name) where kind is 'tools'|'resources'|'other'."""
        if method.upper() != "POST":
            return "other", None
        if not body_bytes:
            return "other", None
        with contextlib.suppress(Exception):
            import json as _json

            payload = _json.loads(body_bytes)
            rpc_method = str(payload.get("method", ""))
            if rpc_method == "tools/call":
                params = payload.get("params", {}) or {}
                tool_name = params.get("name")
                return "tools", tool_name if isinstance(tool_name, str) else None
            if rpc_method.startswith("resources/"):
                return "resources", None
            return "other", None
        return "other", None

    @staticmethod
    def _coerce_rpm(value: object, default: int) -> int:
        # An explicit 0 disables the limit and must survive (#213); only a
        # missing/None value falls back to the default. Use a None check rather
        # than ``value or default`` (which would turn 0 into ``default``).
        if value is None:
            return default
        with contextlib.suppress(Exception):
            return int(value)
        return default

    def _rate_limits_for(self, kind: str) -> tuple[int, int]:
        # return (per_minute, burst)
        if kind == "tools":
            rpm = self._coerce_rpm(getattr(self.settings.http, "rate_limit_tools_per_minute", 60), 60)
            burst = int(getattr(self.settings.http, "rate_limit_tools_burst", 0) or 0)
        elif kind == "resources":
            rpm = self._coerce_rpm(getattr(self.settings.http, "rate_limit_resources_per_minute", 120), 120)
            burst = int(getattr(self.settings.http, "rate_limit_resources_burst", 0) or 0)
        else:
            rpm = self._coerce_rpm(getattr(self.settings.http, "rate_limit_per_minute", 60), 60)
            burst = 0
        # rpm <= 0 means "disabled" (handled by _consume_bucket); don't synthesize
        # a positive burst that would re-enable limiting.
        burst = int(burst) if burst > 0 else max(1, rpm)
        return rpm, burst

    async def _consume_bucket(self, key: str, per_minute: int, burst: int) -> bool:
        """Return True if token granted, False if limited."""
        if per_minute <= 0:
            return True
        rate_per_sec = per_minute / 60.0
        now = self._monotonic()

        # Redis backend
        if self._redis is not None:
            try:
                lua = (
                    "local key = KEYS[1]\n"
                    "local now = tonumber(ARGV[1])\n"
                    "local rate = tonumber(ARGV[2])\n"
                    "local burst = tonumber(ARGV[3])\n"
                    "local state = redis.call('HMGET', key, 'tokens', 'ts')\n"
                    "local tokens = tonumber(state[1]) or burst\n"
                    "local ts = tonumber(state[2]) or now\n"
                    "local delta = now - ts\n"
                    "tokens = math.min(burst, tokens + delta * rate)\n"
                    "local allowed = 0\n"
                    "if tokens >= 1 then\n"
                    "  tokens = tokens - 1\n"
                    "  allowed = 1\n"
                    "end\n"
                    "redis.call('HMSET', key, 'tokens', tokens, 'ts', now)\n"
                    "redis.call('EXPIRE', key, math.ceil(burst / math.max(rate, 0.001)))\n"
                    "return allowed\n"
                )
                allowed = await self._redis.eval(lua, 1, f"rl:{key}", now, rate_per_sec, burst)
                return bool(int(allowed or 0) == 1)
            except Exception:
                # Fallback to memory on Redis failure
                pass

        # In-memory token bucket
        tokens, ts = self._buckets.get(key, (float(burst), now))
        elapsed = max(0.0, now - ts)
        tokens = min(float(burst), tokens + elapsed * rate_per_sec)
        if tokens < 1.0:
            self._buckets[key] = (tokens, now)
            return False
        tokens -= 1.0
        self._buckets[key] = (tokens, now)
        return True

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        # Perform periodic cleanup of in-memory rate limit buckets
        if self._redis is None:
            now = self._monotonic()
            if now - self._last_cleanup > 60.0:
                self._cleanup_buckets(now)
                self._last_cleanup = now

        # Allow CORS preflight and health endpoints
        if (
            request.method == "OPTIONS"
            or request.url.path.startswith("/health/")
            or request.url.path == "/api/health"
            or request.url.path in PUBLIC_CLIENT_PATHS
        ):
            return await call_next(request)

        # Only read/patch body for POST requests. GET (including SSE) must not receive http.request messages.
        body_bytes = b""
        if request.method.upper() == "POST":
            try:
                body_bytes = await request.body()
                body_sent = False

                async def _receive() -> dict:
                    nonlocal body_sent
                    if body_sent:
                        return {"type": "http.request", "body": b"", "more_body": False}
                    body_sent = True
                    return {"type": "http.request", "body": body_bytes, "more_body": False}

                cast(Any, request)._receive = _receive
            except Exception:
                body_bytes = b""

        kind, tool_name = self._classify_request(request.url.path, request.method, body_bytes)

        # JWT auth (if enabled)
        if self._jwt_enabled:
            auth_header = request.headers.get("Authorization", "")
            # #210: when JWT is enabled, a valid *static* bearer is still accepted
            # as the OR-alternative to a JWT (the outer BearerAuthMiddleware defers
            # Bearer requests here without distinguishing the two). Check it first so
            # static-bearer clients keep working once JWT is turned on; a static
            # bearer is treated exactly as it is when JWT is disabled (default role).
            static_token = getattr(self.settings.http, "bearer_token", "") or ""
            token_owner = None
            if static_token and auth_header.startswith(f"Bearer {ui_auth.TOKEN_PREFIX}"):
                token_owner = await ui_auth.api_token_owner(auth_header[len("Bearer ") :].strip())
            if static_token and (
                hmac.compare_digest(auth_header, f"Bearer {static_token}")
                or await _ui_request_authenticated(request)
                or token_owner is not None
            ):
                if token_owner is not None:
                    request.state.agent_mail_owner = token_owner
                roles = {self._default_role}
            else:
                if not auth_header.startswith("Bearer "):
                    return JSONResponse({"detail": "Unauthorized"}, status_code=status.HTTP_401_UNAUTHORIZED)
                token = auth_header.split(" ", 1)[1].strip()
                claims_dict = await self._decode_jwt(token)
                if claims_dict is None:
                    return JSONResponse({"detail": "Unauthorized"}, status_code=status.HTTP_401_UNAUTHORIZED)
                claims = cast(dict[str, Any], claims_dict)
                request.state.jwt_claims = claims
                roles_raw = claims.get(self.settings.http.jwt_role_claim, [])
                if isinstance(roles_raw, str):
                    roles = {roles_raw}
                elif isinstance(roles_raw, (list, tuple)):
                    roles = {str(r) for r in roles_raw}
                else:
                    roles = set()
                if not roles:
                    roles = {self._default_role}
        else:
            roles = {self._default_role}
            # Elevate localhost to writer when unauthenticated localhost is allowed
            if _localhost_bypass_allowed(
                request,
                allow_localhost=bool(getattr(self.settings.http, "allow_localhost_unauthenticated", False)),
            ):
                roles.add("writer")

        # RBAC enforcement (skip for localhost when allowed)
        is_local_ok = _localhost_bypass_allowed(
            request,
            allow_localhost=bool(getattr(self.settings.http, "allow_localhost_unauthenticated", False)),
        )
        if self._rbac_enabled and not is_local_ok and kind in {"tools", "resources"}:
            is_reader = bool(roles & self._reader_roles)
            is_writer = bool(roles & self._writer_roles) or (not roles)
            if kind == "resources":
                pass  # readers allowed
            elif kind == "tools":
                if not tool_name:
                    # Without name, assume write-required to be safe
                    if not is_writer:
                        return JSONResponse({"detail": "Forbidden"}, status_code=status.HTTP_403_FORBIDDEN)
                else:
                    if tool_name in self._readonly_tools:
                        if not is_reader and not is_writer:
                            return JSONResponse({"detail": "Forbidden"}, status_code=status.HTTP_403_FORBIDDEN)
                    else:
                        if not is_writer:
                            return JSONResponse({"detail": "Forbidden"}, status_code=status.HTTP_403_FORBIDDEN)

        # Rate limiting
        if self.settings.http.rate_limit_enabled:
            rpm, burst = self._rate_limits_for(kind)
            identity = request.client.host if request.client else "ip-unknown"
            # Prefer stable subject from JWT if present
            with contextlib.suppress(Exception):
                maybe_claims = getattr(request.state, "jwt_claims", None)
                if isinstance(maybe_claims, dict):
                    sub = maybe_claims.get("sub")
                    if isinstance(sub, str) and sub:
                        identity = f"sub:{sub}"
            endpoint = tool_name or "*"
            key = f"{kind}:{endpoint}:{identity}"
            allowed = await self._consume_bucket(key, rpm, burst)
            if not allowed:
                return JSONResponse({"detail": "Rate limit exceeded"}, status_code=status.HTTP_429_TOO_MANY_REQUESTS)

        return await call_next(request)


async def readiness_check() -> None:
    await ensure_schema()
    async with get_session() as session:
        await session.execute(text("SELECT 1"))

    # Fail readiness if FD usage from lockfile leaks is critically high.
    # This gives orchestrators a signal to restart the process before it
    # becomes completely wedged (issue #116).
    current, limit = get_fd_usage()
    if current >= 0 and limit > 0:
        headroom_pct = (limit - current) / limit
        if headroom_pct < 0.10:
            lock_stats = get_lock_telemetry()
            raise RuntimeError(
                f"FD exhaustion imminent: {current}/{limit} FDs in use "
                f"({round(headroom_pct * 100, 1)}% headroom). "
                f"Lock telemetry: {lock_stats}"
            )


def create_app() -> FastAPI:
    """Zero-argument ASGI app factory for ``uvicorn ... --factory`` (#214).

    ``build_http_app`` requires a ``Settings`` argument, so it cannot be used
    directly as a uvicorn ``--factory`` target. This wrapper resolves settings
    from the environment and builds the app, matching the documented command.
    """
    return build_http_app(get_settings())


def build_http_app(settings: Settings, server=None) -> FastAPI:
    # Configure logging once
    _configure_logging(settings)
    if server is None:
        server = build_mcp_server()

    # Build MCP HTTP sub-app with stateless mode for ASGI test transports
    mcp_http_app = cast(_FastMCPHttpApp, server).http_app(
        path="/",
        stateless_http=True,
        json_response=True,
    )

    # Second, STATEFUL MCP sub-app (issue #250): stateless mode creates a new
    # transport per request and never issues an ``Mcp-Session-Id`` header, so
    # session-bound agent authentication (#148) could never persist across
    # HTTP tool calls — ``create_agent_identity(return_registration_token=false)``
    # followed by any protected call failed with AUTHENTICATION_REQUIRED.
    # A bare flip to ``stateless_http=False`` would break handshake-skipping
    # clients (e.g. ntm's HTTP client), so we mount BOTH: the stateful app at
    # '/mcp' for spec-compliant MCP clients that keep a session, and the
    # stateless app at '/api' (and the configured base) for one-shot clients.
    mcp_stateful_http_app = cast(_FastMCPHttpApp, server).http_app(
        path="/",
        stateless_http=False,
        json_response=True,
    )

    # no-op wrapper removed; using explicit stateless adapter below

    # Background workers lifecycle
    async def _startup() -> None:  # pragma: no cover - service lifecycle
        # Note: no early return here -- the FD health monitor always runs,
        # even when optional workers are disabled by feature flags.

        async def _worker_cleanup() -> None:
            while True:
                try:
                    await ensure_schema()
                    async with get_session() as session:
                        rows = await session.execute(text("SELECT DISTINCT project_id FROM file_reservations"))
                        pids = [r[0] for r in rows.fetchall() if r[0] is not None]
                    released_total = 0
                    for pid in pids:
                        with contextlib.suppress(Exception):
                            stale = await _expire_stale_file_reservations(pid)
                            released_total += len(stale)
                    try:
                        rich_console = importlib.import_module("rich.console")
                        rich_panel = importlib.import_module("rich.panel")
                        Console = rich_console.Console
                        Panel = rich_panel.Panel
                        Console().print(
                            Panel.fit(
                                f"projects_scanned={len(pids)} released={released_total}",
                                title="File Reservations Cleanup",
                                border_style="cyan",
                            )
                        )
                    except Exception:
                        pass
                    with contextlib.suppress(Exception):
                        structlog.get_logger("tasks").info(
                            "file_reservations_cleanup",
                            projects_scanned=len(pids),
                            stale_released=released_total,
                        )
                except Exception:
                    pass
                await asyncio.sleep(settings.file_reservations_cleanup_interval_seconds)

        async def _worker_ack_ttl() -> None:
            import datetime as _dt

            while True:
                try:
                    await ensure_schema()
                    async with get_session() as session:
                        result = await session.execute(
                            text(
                                """
                            SELECT m.id, m.project_id, m.created_ts, mr.agent_id
                            FROM messages m
                            JOIN message_recipients mr ON mr.message_id = m.id
                            WHERE m.ack_required = 1 AND mr.ack_ts IS NULL
                            """
                            )
                        )
                        rows = result.fetchall()
                    now = _dt.datetime.now(_dt.timezone.utc)
                    now_naive = now.replace(tzinfo=None)
                    for mid, project_id, created_ts, agent_id in rows:
                        # Normalize to timezone-aware UTC before arithmetic; SQLite may yield naive datetimes
                        ts = created_ts
                        if getattr(ts, "tzinfo", None) is None or ts.tzinfo.utcoffset(ts) is None:
                            ts = ts.replace(tzinfo=_dt.timezone.utc)
                        else:
                            ts = ts.astimezone(_dt.timezone.utc)
                        age = (now - ts).total_seconds()
                        if age >= settings.ack_ttl_seconds:
                            try:
                                rich_console = importlib.import_module("rich.console")
                                rich_panel = importlib.import_module("rich.panel")
                                rich_text = importlib.import_module("rich.text")
                                Console = rich_console.Console
                                Panel = rich_panel.Panel
                                Text = rich_text.Text
                                con = Console()
                                body = Text.assemble(
                                    ("message_id: ", "cyan"),
                                    (str(mid), "white"),
                                    "\n",
                                    ("agent_id: ", "cyan"),
                                    (str(agent_id), "white"),
                                    "\n",
                                    ("project_id: ", "cyan"),
                                    (str(project_id), "white"),
                                    "\n",
                                    ("age_s: ", "cyan"),
                                    (str(int(age)), "white"),
                                    "\n",
                                    ("ttl_s: ", "cyan"),
                                    (str(settings.ack_ttl_seconds), "white"),
                                )
                                con.print(Panel(body, title="ACK Overdue", border_style="red"))
                            except Exception:
                                print(
                                    f"ack-warning message_id={mid} project_id={project_id} agent_id={agent_id} age_s={int(age)} ttl_s={settings.ack_ttl_seconds}"
                                )
                            with contextlib.suppress(Exception):
                                structlog.get_logger("tasks").warning(
                                    "ack_overdue",
                                    message_id=str(mid),
                                    project_id=str(project_id),
                                    agent_id=str(agent_id),
                                    age_s=int(age),
                                    ttl_s=int(settings.ack_ttl_seconds),
                                )
                            if settings.ack_escalation_enabled:
                                mode = (settings.ack_escalation_mode or "log").lower()
                                if mode == "file_reservation":
                                    try:
                                        y_dir = created_ts.strftime("%Y")
                                        m_dir = created_ts.strftime("%m")
                                        # Resolve recipient name
                                        async with get_session() as s_lookup:
                                            name_row = await s_lookup.execute(
                                                text("SELECT name FROM agents WHERE id = :aid"), {"aid": agent_id}
                                            )
                                            name_res = name_row.fetchone()
                                        recipient_name = name_res[0] if name_res and name_res[0] else "*"
                                        pattern = (
                                            f"agents/{recipient_name}/inbox/{y_dir}/{m_dir}/*.md"
                                            if recipient_name != "*"
                                            else f"agents/*/inbox/{y_dir}/{m_dir}/*.md"
                                        )
                                        project_slug = await _project_slug_from_id(project_id)
                                        holder_agent_id = int(agent_id)
                                        holder_agent_name = recipient_name
                                        if settings.ack_escalation_claim_holder_name:
                                            claim_name = settings.ack_escalation_claim_holder_name
                                            holder_agent_id, holder_agent_name = await _ensure_ack_escalation_holder(
                                                settings=settings,
                                                project_id=int(project_id),
                                                project_slug=project_slug,
                                                recipient_agent_id=int(agent_id),
                                                recipient_name=recipient_name,
                                                claim_name=claim_name,
                                                now=now,
                                                now_naive=now_naive,
                                            )
                                        async with get_session() as s2:
                                            await s2.execute(
                                                text(
                                                    """
                                                INSERT INTO file_reservations(project_id, agent_id, path_pattern, exclusive, reason, created_ts, expires_ts)
                                                VALUES (:pid, :holder, :pattern, :exclusive, :reason, :cts, :ets)
                                                """
                                                ),
                                                {
                                                    "pid": project_id,
                                                    "holder": holder_agent_id,
                                                    "pattern": pattern,
                                                    "exclusive": 1 if settings.ack_escalation_claim_exclusive else 0,
                                                    "reason": "ack-overdue",
                                                    "cts": now_naive,
                                                    "ets": now_naive
                                                    + _dt.timedelta(seconds=settings.ack_escalation_claim_ttl_seconds),
                                                },
                                            )
                                            await s2.commit()
                                        # Also write JSON artifact to archive
                                        if not project_slug:
                                            raise ValueError(f"Project id {project_id} has no slug; cannot write archive artifacts.")
                                        archive = await ensure_archive(settings, project_slug)
                                        expires_at = now + _dt.timedelta(
                                            seconds=settings.ack_escalation_claim_ttl_seconds
                                        )
                                        async with archive_write_lock(archive):
                                            await write_file_reservation_record(
                                                archive,
                                                {
                                                    "project": project_slug,
                                                    "agent": holder_agent_name,
                                                    "path_pattern": pattern,
                                                    "exclusive": settings.ack_escalation_claim_exclusive,
                                                    "reason": "ack-overdue",
                                                    "created_ts": now.isoformat(),
                                                    "expires_ts": expires_at.isoformat(),
                                                },
                                            )
                                    except Exception:
                                        pass
                except Exception:
                    pass
                await asyncio.sleep(settings.ack_ttl_scan_interval_seconds)

        async def _worker_tool_metrics() -> None:
            log = structlog.get_logger("tool.metrics")
            while True:
                try:
                    snapshot = _tool_metrics_snapshot()
                    if snapshot:
                        log.info("tool_metrics_snapshot", tools=snapshot)
                except Exception:
                    pass
                await asyncio.sleep(max(5, settings.tool_metrics_emit_interval_seconds))

        async def _worker_retention_quota() -> None:
            while True:
                with contextlib.suppress(Exception):
                    report = await _collect_retention_quota_report(settings)
                    structlog.get_logger("maintenance").info(
                        "retention_quota_report",
                        **report,
                    )
                    # Quota alerts
                    limit_b = int(settings.quota_attachments_limit_bytes)
                    inbox_limit = int(settings.quota_inbox_limit_count)
                    if limit_b > 0:
                        for proj, used in report["per_project_attach"].items():
                            if used >= limit_b:
                                structlog.get_logger("maintenance").warning(
                                    "quota_attachments_exceeded", project=proj, used_bytes=used, limit_bytes=limit_b
                                )
                    if inbox_limit > 0:
                        for proj, cnt in report["per_project_inbox_counts"].items():
                            if cnt >= inbox_limit:
                                structlog.get_logger("maintenance").warning(
                                    "quota_inbox_exceeded", project=proj, inbox_count=cnt, limit=inbox_limit
                                )
                await asyncio.sleep(max(60, settings.retention_report_interval_seconds))

        async def _worker_fd_health() -> None:
            """Periodic file descriptor health monitor.

            Checks FD headroom every 30 seconds and proactively cleans up
            resources when headroom drops below safe thresholds. This prevents
            the EMFILE -> socket closed -> unreachable cascade that occurs
            under sustained multi-agent load.

            Also monitors lockfile FD leaks (issue #116) and cleans up
            deleted-but-open .lock file descriptors.

            Thresholds:
            - 30% headroom: warning logged
            - 20% headroom: proactive cleanup triggered (includes lockfile FDs)
            - 15% headroom: error logged, aggressive cleanup
            """
            _fd_logger = structlog.get_logger("fd_health")
            while True:
                try:
                    current, limit = get_fd_usage()
                    if current >= 0 and limit > 0:
                        headroom_pct = (limit - current) / limit
                        cache_stats = get_repo_cache_stats()
                        lock_stats = get_lock_telemetry()

                        if headroom_pct < 0.15:
                            # Critical: aggressive cleanup
                            _fd_logger.error(
                                "fd_health.critical",
                                current_fds=current,
                                fd_limit=limit,
                                headroom_pct=round(headroom_pct * 100, 1),
                                repo_cache=cache_stats,
                                lock_telemetry=lock_stats,
                            )
                            freed = proactive_fd_cleanup(threshold=limit)
                            if freed:
                                _fd_logger.warning(
                                    "fd_health.emergency_cleanup",
                                    freed=freed,
                                    new_headroom=get_fd_headroom(),
                                )
                        elif headroom_pct < 0.20:
                            # Low: proactive cleanup
                            _fd_logger.warning(
                                "fd_health.low",
                                current_fds=current,
                                fd_limit=limit,
                                headroom_pct=round(headroom_pct * 100, 1),
                                repo_cache=cache_stats,
                                lock_telemetry=lock_stats,
                            )
                            freed = proactive_fd_cleanup(threshold=int(limit * 0.25))
                            if freed:
                                _fd_logger.info(
                                    "fd_health.proactive_cleanup",
                                    freed=freed,
                                    new_headroom=get_fd_headroom(),
                                )
                        elif headroom_pct < 0.30:
                            # Warning only
                            _fd_logger.warning(
                                "fd_health.warning",
                                current_fds=current,
                                fd_limit=limit,
                                headroom_pct=round(headroom_pct * 100, 1),
                                repo_cache=cache_stats,
                                lock_telemetry=lock_stats,
                            )
                except Exception:
                    pass
                await asyncio.sleep(30)

        async def _worker_auto_retire_stale_agents() -> None:
            log = structlog.get_logger("maintenance.auto_retire")
            interval = max(60, int(settings.auto_retire_stale_agents_interval_seconds))
            threshold = max(60, int(settings.auto_retire_stale_agents_threshold_seconds))
            while True:
                with contextlib.suppress(Exception):
                    retired = await sweep_stale_agents(threshold_seconds=threshold)
                    if retired:
                        log.info(
                            "auto_retired_stale_agents",
                            count=len(retired),
                            threshold_seconds=threshold,
                            agents=[
                                {
                                    "agent": entry["agent_name"],
                                    "project": entry["project_key"],
                                    "last_active_ts": entry["last_active_ts"],
                                }
                                for entry in retired
                            ],
                        )
                await asyncio.sleep(interval)

        async def _worker_presence_sweep() -> None:
            # Agents whose heartbeats stopped go offline, which the live stream shows.
            while True:
                with contextlib.suppress(Exception):
                    await sweep_presence()
                await asyncio.sleep(60)

        async def _worker_audit_prune() -> None:
            # Daily: drop audit entries older than AUDIT_RETENTION_DAYS (0 keeps them forever).
            while True:
                with contextlib.suppress(Exception):
                    await audit.prune(settings.http.audit_retention_days)
                await asyncio.sleep(24 * 3600)

        async def _worker_scheduled_backup() -> None:
            # Every 30 min, take a lossless archive if the last scheduled one is older than
            # BACKUP_INTERVAL_HOURS (a file lock lets one worker do it). Runs off the event loop.
            from .cli import run_scheduled_backup

            log = structlog.get_logger("maintenance.backup")
            await asyncio.sleep(60)  # let startup settle
            while True:
                try:
                    made = await asyncio.to_thread(
                        run_scheduled_backup,
                        interval_hours=settings.storage.backup_interval_hours,
                        keep=settings.storage.backup_keep,
                    )
                    if made is not None:
                        log.info("scheduled_backup_created", path=str(made), size_bytes=made.stat().st_size)
                except Exception as exc:
                    log.error("scheduled_backup_failed", error=repr(exc))
                await asyncio.sleep(30 * 60)

        tasks = []
        # FD health monitor always runs - it's critical for preventing EMFILE cascades
        tasks.append(asyncio.create_task(_worker_fd_health()))
        tasks.append(asyncio.create_task(_worker_presence_sweep()))
        tasks.append(asyncio.create_task(_worker_audit_prune()))
        if settings.storage.backup_interval_hours > 0:
            tasks.append(asyncio.create_task(_worker_scheduled_backup()))
        if settings.file_reservations_cleanup_enabled:
            tasks.append(asyncio.create_task(_worker_cleanup()))
        if settings.ack_ttl_enabled:
            tasks.append(asyncio.create_task(_worker_ack_ttl()))
        if settings.tool_metrics_emit_enabled:
            tasks.append(asyncio.create_task(_worker_tool_metrics()))
        if settings.retention_report_enabled or settings.quota_enabled:
            tasks.append(asyncio.create_task(_worker_retention_quota()))
        if settings.auto_retire_stale_agents_enabled:
            tasks.append(asyncio.create_task(_worker_auto_retire_stale_agents()))
        fastapi_app.state._background_tasks = tasks

    async def _shutdown() -> None:  # pragma: no cover - service lifecycle
        tasks = getattr(fastapi_app.state, "_background_tasks", [])
        for task in tasks:
            task.cancel()
        # Await cancelled tasks with a timeout to prevent shutdown hangs
        # (aiosqlite cancellation can block indefinitely)
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.wait(tasks, timeout=5.0)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan_context(app: FastAPI):
        # Ensure both mounted MCP apps initialize their internal task groups
        # (each http_app() call owns an independent StreamableHTTPSessionManager).
        mcp_lifespan_app = cast(_FastAPILifespan, mcp_http_app)
        mcp_stateful_lifespan_app = cast(_FastAPILifespan, mcp_stateful_http_app)
        async with mcp_lifespan_app.lifespan(mcp_http_app):
            async with mcp_stateful_lifespan_app.lifespan(mcp_stateful_http_app):
                await _startup()
                try:
                    yield
                finally:
                    await _shutdown()

    # Now construct FastAPI with the composed lifespan so ASGI transports run it.
    # Give the app a real title/version so the auto-generated /openapi.json has a
    # proper `info` block (derive the version from installed package metadata,
    # mirroring cli._package_version; never hardcode a value that could drift).
    def _package_version() -> str:
        import importlib.metadata as _importlib_metadata

        try:
            return _importlib_metadata.version("mcp-agent-mail")
        except _importlib_metadata.PackageNotFoundError:  # pragma: no cover - dev installs
            return "0.0.0+local"

    fastapi_app = FastAPI(
        title="MCP Agent Mail",
        version=_package_version(),
        lifespan=lifespan_context,
    )

    # Simple request logging (configurable)
    if settings.http.request_log_enabled:
        import time as _time

        class RequestLoggingMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
                start = _time.time()
                method = request.method
                path = request.url.path
                client = request.client.host if request.client else "-"
                response = None
                exc: BaseException | None = None
                try:
                    response = await call_next(request)
                    return response
                except BaseException as err:
                    exc = err
                    raise
                finally:
                    # Always emit a log line, even when the handler raised (#215).
                    dur_ms = int((_time.time() - start) * 1000)
                    status_code = getattr(response, "status_code", 0) if response is not None else 500
                    with contextlib.suppress(Exception):
                        log = structlog.get_logger("http")
                        if exc is not None:
                            log.error(
                                "request",
                                method=method,
                                path=path,
                                status=status_code,
                                duration_ms=dur_ms,
                                client_ip=client,
                                error=repr(exc),
                            )
                        else:
                            log.info(
                                "request",
                                method=method,
                                path=path,
                                status=status_code,
                                duration_ms=dur_ms,
                                client_ip=client,
                            )
                    try:
                        rich_console = importlib.import_module("rich.console")
                        rich_panel = importlib.import_module("rich.panel")
                        rich_text = importlib.import_module("rich.text")
                        Console = rich_console.Console
                        Panel = rich_panel.Panel
                        Text = rich_text.Text
                        console = Console(width=100)
                        title = Text.assemble(
                            (method, "bold blue"),
                            ("  "),
                            (path, "bold white"),
                            ("  "),
                            (f"{status_code}", "bold green" if 200 <= status_code < 400 else "bold red"),
                            ("  "),
                            (f"{dur_ms}ms", "bold yellow"),
                        )
                        body = Text.assemble(
                            ("client: ", "cyan"),
                            (client, "white"),
                        )
                        if exc is not None:
                            body = Text.assemble(body, "\n", ("error: ", "cyan"), (repr(exc), "red"))
                        console.print(Panel(body, title=title, border_style="dim"))
                    except Exception:
                        suffix = f" error={exc!r}" if exc is not None else ""
                        print(
                            f"http method={method} path={path} status={status_code} ms={dur_ms} client={client}{suffix}"
                        )

        app_any = cast(Any, fastapi_app)
        app_any.add_middleware(RequestLoggingMiddleware)

    # Unified JWT/RBAC and robust rate limiter middleware
    if (
        settings.http.rate_limit_enabled
        or getattr(settings.http, "jwt_enabled", False)
        or getattr(settings.http, "rbac_enabled", True)
    ):
        app_any = cast(Any, fastapi_app)
        app_any.add_middleware(SecurityAndRateLimitMiddleware, settings=settings)
    # Added before bearer auth, so it runs after it: unauthenticated callers get 401 first.
    if settings.http.client_min_version:
        cast(Any, fastapi_app).add_middleware(ClientVersionMiddleware, minimum=settings.http.client_min_version)
    # Bearer auth for non-localhost only; allow localhost unauth optionally for seamless local dev
    if settings.http.bearer_token:
        from typing import Any as _Any, cast as _cast  # local type-only import
        app_any = _cast(_Any, fastapi_app)
        app_any.add_middleware(
            BearerAuthMiddleware,
            token=settings.http.bearer_token,
            allow_localhost=bool(getattr(settings.http, "allow_localhost_unauthenticated", False)),
            jwt_enabled=bool(getattr(settings.http, "jwt_enabled", False)),
        )

    # Optional CORS
    if settings.cors.enabled:
        from typing import Any as _Any, cast as _cast  # local type-only import
        app_any2 = _cast(_Any, fastapi_app)
        app_any2.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors.origins or [],
            allow_credentials=settings.cors.allow_credentials,
            allow_methods=settings.cors.allow_methods or ["*"],
            allow_headers=settings.cors.allow_headers or ["*"],
        )

    # Agent-machine installer (#11): `curl -fsSL https://<server>/install.sh | sh -s -- --project ... --agent ...`
    @fastapi_app.get("/install.sh")
    async def install_script(request: Request) -> Response:
        """The installer, with this server's public URL filled in (as uvicorn sees it behind the proxy)."""
        template = await asyncio.to_thread((Path(__file__).parent / "templates" / "install.sh").read_text)
        server_url = str(request.base_url).rstrip("/")
        return Response(
            template.replace(INSTALLER_PLACEHOLDER, server_url),
            media_type="text/x-shellscript; charset=utf-8",
            headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
        )

    @fastapi_app.get("/client/{name}")
    async def client_script(name: str) -> Response:
        """One of the client scripts the installer downloads; nothing else is served from here."""
        if name not in CLIENT_SCRIPTS:
            raise HTTPException(status_code=404, detail="Not found")
        body = await asyncio.to_thread((CLIENT_SCRIPTS_DIR / name).read_bytes)
        return Response(
            body,
            media_type="text/x-python; charset=utf-8",
            headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
        )

    # Health endpoints
    @fastapi_app.get("/health/liveness")
    async def liveness() -> JSONResponse:
        return JSONResponse({"status": "alive"})

    @fastapi_app.get("/health/readiness")
    async def readiness() -> JSONResponse:
        try:
            await readiness_check()
        except Exception as exc:
            try:
                rich_console = importlib.import_module("rich.console")
                rich_panel = importlib.import_module("rich.panel")
                Console = rich_console.Console
                Panel = rich_panel.Panel
                Console().print(Panel.fit(str(exc), title="Readiness Error", border_style="red"))
            except Exception:
                pass
            with contextlib.suppress(Exception):
                structlog.get_logger("health").error("readiness_error", error=str(exc))
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
        return JSONResponse({"status": "ready"})

    @fastapi_app.get("/api/health")
    async def api_health_bypass() -> JSONResponse:
        """Lightweight health probe that bypasses the MCP transport layer.

        Returns immediately without touching the database or connection pool,
        so it stays responsive even when the MCP ASGI pipeline is saturated
        under heavy multi-agent load.
        """
        return JSONResponse({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})

    def _oauth_metadata_disabled_response() -> JSONResponse:
        return JSONResponse({"mcp_oauth": False}, status_code=404)

    def _register_oauth_metadata_disabled(path: str) -> None:
        async def _oauth_metadata_disabled() -> JSONResponse:
            return _oauth_metadata_disabled_response()

        fastapi_app.add_api_route(path, _oauth_metadata_disabled, methods=["GET"], include_in_schema=False)

    # Thin ASGI wrapper that normalizes Accept / Content-Type headers for
    # MCP clients (some omit Accept entirely) and then delegates to the
    # SDK's native mcp_http_app which properly coordinates server lifecycle,
    # request handling, and session management via StreamableHTTPSessionManager.
    #
    # In production the parent FastAPI lifespan initializes the session manager
    # task group before any requests arrive.  In test environments (httpx
    # ASGITransport) no lifespan events are sent, so the wrapper lazily enters
    # the MCP app's lifespan on first request to avoid "Task group not
    # initialized" errors.
    class _HeaderFixupMCPApp:
        """Normalize headers then delegate to the native MCP HTTP app."""

        def __init__(self, native_app: FastAPI) -> None:
            self._app = native_app
            self._lifespan_entered = False
            self._lifespan_cm: Any = None
            self._lifespan_lock: asyncio.Lock | None = None

        async def _ensure_lifespan(self) -> None:
            """Lazily enter the MCP app's lifespan if not already running.

            This handles test environments where ASGI lifespan events are never
            sent (e.g. httpx ASGITransport).  In production the parent app's
            lifespan context already calls mcp_http_app.lifespan, so the
            session manager's task group will already be initialized and this
            method is a fast no-op.

            Uses double-check locking to prevent concurrent requests from
            entering the lifespan context manager twice.
            """
            if self._lifespan_entered:
                return
            # Lazily create the lock (must be in async context for the
            # correct event loop).
            if self._lifespan_lock is None:
                self._lifespan_lock = asyncio.Lock()
            async with self._lifespan_lock:
                if self._lifespan_entered:
                    return
                # Check if the session manager is already running (production path)
                session_mgr = getattr(self._app.state, "session_manager", None)
                if session_mgr is None:
                    # Try to find it via route endpoint
                    for route in getattr(self._app, "routes", []):
                        endpoint = getattr(route, "endpoint", None)
                        sm = getattr(endpoint, "session_manager", None)
                        if sm is not None:
                            session_mgr = sm
                            break
                if session_mgr is not None and getattr(session_mgr, "_task_group", None) is not None:
                    self._lifespan_entered = True
                    return
                # Enter the MCP app's lifespan (test path)
                mcp_lifespan_app = cast(_FastAPILifespan, self._app)
                self._lifespan_cm = mcp_lifespan_app.lifespan(self._app)
                await self._lifespan_cm.__aenter__()
                self._lifespan_entered = True

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
            if scope.get("type") != "http":
                # Delegate non-HTTP scopes (e.g. lifespan) directly
                await self._app(scope, receive, send)
                return

            await self._ensure_lifespan()

            headers = list(scope.get("headers") or [])

            def _has_header(key: bytes) -> bool:
                lk = key.lower()
                return any(h[0].lower() == lk for h in headers)

            # Ensure both JSON and SSE are accepted; httpx defaults no Accept header
            headers = [(k, v) for (k, v) in headers if k.lower() != b"accept"]
            headers.append((b"accept", b"application/json, text/event-stream"))
            if scope.get("method") == "POST" and not _has_header(b"content-type"):
                headers.append((b"content-type", b"application/json"))
            new_scope = dict(scope)
            new_scope["headers"] = headers

            await self._app(new_scope, receive, send)

    # Mount at both '/base' and '/base/' to tolerate either form from clients/tests.
    # Also mount compatibility aliases for both '/api' and '/mcp' regardless of configured base.
    mount_base = settings.http.path or "/api"
    if not mount_base.startswith("/"):
        mount_base = "/" + mount_base
    base_no_slash = mount_base.rstrip("/") or "/"
    base_with_slash = base_no_slash if base_no_slash == "/" else base_no_slash + "/"
    stateless_app = _HeaderFixupMCPApp(mcp_http_app)
    stateful_app = _HeaderFixupMCPApp(mcp_stateful_http_app)

    # Path -> app mapping (issue #250): the '/mcp' compat alias is the
    # stateful, Mcp-Session-Id-issuing endpoint; '/api' and the configured
    # base stay stateless for handshake-skipping one-shot clients (e.g. ntm).
    # The CONFIGURED base always keeps the legacy stateless behavior, even if
    # an operator points it at '/mcp' — an explicit HTTP_PATH is a promise to
    # existing clients of that deployment, so we never change its semantics.
    def _app_for_mount(path: str) -> _HeaderFixupMCPApp:
        normalized = path.rstrip("/") or "/"
        if normalized == "/mcp" and base_no_slash != "/mcp":
            return stateful_app
        return stateless_app

    mount_paths = [base_no_slash, base_with_slash]
    for compat_base in ("/api", "/mcp"):
        compat_no_slash = compat_base.rstrip("/") or "/"
        compat_with_slash = compat_no_slash if compat_no_slash == "/" else compat_no_slash + "/"
        if compat_no_slash not in mount_paths:
            mount_paths.append(compat_no_slash)
        if compat_with_slash not in mount_paths:
            mount_paths.append(compat_with_slash)

    oauth_metadata_paths: set[str] = set()

    def _add_oauth_metadata_path(path: str) -> None:
        normalized = path.rstrip("/") or "/"
        oauth_metadata_paths.add(normalized)
        if normalized != "/":
            oauth_metadata_paths.add(f"{normalized}/")

    _add_oauth_metadata_path("/.well-known/oauth-authorization-server")
    _add_oauth_metadata_path("/.well-known/oauth-authorization-server/mcp")
    for mount_path in mount_paths:
        normalized = mount_path.rstrip("/") or "/"
        if normalized == "/":
            continue
        _add_oauth_metadata_path(f"{normalized}/.well-known/oauth-authorization-server")
        _add_oauth_metadata_path(f"{normalized}/.well-known/oauth-authorization-server/mcp")
        _add_oauth_metadata_path(f"/.well-known/oauth-authorization-server{normalized}")
    for path in sorted(oauth_metadata_paths):
        _register_oauth_metadata_disabled(path)

    for mount_path in mount_paths:
        with contextlib.suppress(Exception):
            fastapi_app.mount(mount_path, _app_for_mount(mount_path))

    # Expose composed lifespan via router
    fastapi_app.router.lifespan_context = lifespan_context

    # Add direct routes at no-slash base paths to tolerate clients omitting trailing slashes.
    def _register_base_passthrough(base_path_no_slash: str, base_path_with_slash: str) -> None:
        # Dispatch to the same app that is mounted at this base (issue #250:
        # '/mcp' is stateful, everything else stateless).
        target_app = _app_for_mount(base_path_no_slash)

        @fastapi_app.post(base_path_no_slash)
        async def _base_passthrough(request: Request) -> JSONResponse:
            # Re-dispatch to the mounted MCP app by calling it directly
            response_body: dict[str, Any] = {}
            status_code = 200
            headers: dict[str, str] = {}

            async def _send(message: MutableMapping[str, Any]) -> None:
                nonlocal response_body, status_code, headers
                if message.get("type") == "http.response.start":
                    status_code = int(message.get("status", 200))
                    hdrs = message.get("headers") or []
                    for k, v in hdrs:
                        headers[k.decode("latin1")] = v.decode("latin1")
                elif message.get("type") == "http.response.body":
                    body = message.get("body") or b""
                    try:
                        response_body = json.loads(body.decode("utf-8")) if body else {}
                    except Exception:
                        response_body = {}

            # If localhost and allow_localhost_unauthenticated, synthesize Authorization header automatically
            scope = dict(request.scope)
            if _localhost_bypass_allowed(
                request,
                allow_localhost=bool(settings.http.allow_localhost_unauthenticated),
            ):
                scope_headers = list(scope.get("headers") or [])
                has_auth = any(k.lower() == b"authorization" for k, _ in scope_headers)
                if not has_auth and settings.http.bearer_token:
                    scope_headers.append((b"authorization", f"Bearer {settings.http.bearer_token}".encode("latin1")))
                scope["headers"] = scope_headers
            await target_app(
                {**scope, "path": "/"},  # MCP app expects requests at its root
                request.receive,
                _send,
            )
            return JSONResponse(response_body, status_code=status_code, headers=headers)

    passthrough_pairs: list[tuple[str, str]] = [(base_no_slash, base_with_slash)]
    for compat_base in ("/api", "/mcp"):
        compat_no_slash = compat_base.rstrip("/") or "/"
        compat_with_slash = compat_no_slash if compat_no_slash == "/" else compat_no_slash + "/"
        if (compat_no_slash, compat_with_slash) not in passthrough_pairs:
            passthrough_pairs.append((compat_no_slash, compat_with_slash))
    for no_slash, with_slash in passthrough_pairs:
        _register_base_passthrough(no_slash, with_slash)

    # ----- Simple SSR Mail UI -----
    def _register_mail_ui() -> None:
        import bleach
        import markdown2

        try:
            from bleach.css_sanitizer import CSSSanitizer as _CSSSanitizerImport
        except Exception:  # tinycss2 may be missing; degrade gracefully
            _CSSSanitizer = None
        else:
            _CSSSanitizer = _CSSSanitizerImport
        CSSSanitizer = cast(Any, _CSSSanitizer)
        from jinja2 import Environment, FileSystemLoader, select_autoescape

        templates_root = Path(__file__).resolve().parent / "templates"
        env = Environment(
            loader=FileSystemLoader(str(templates_root)),
            autoescape=select_autoescape(["html", "xml"]),
            enable_async=True,
        )
        # Pages show a sign-out control only when browser login is in force.
        cast(dict[str, Any], env.globals)["ui_login_enabled"] = bool(settings.http.bearer_token)
        # HTML sanitizer (allow safe images and limited CSS)
        _css_sanitizer = (
            CSSSanitizer(
                allowed_css_properties=["color", "background-color", "text-align", "text-decoration", "font-weight"]
            )
            if CSSSanitizer
            else None
        )
        _html_cleaner = bleach.Cleaner(
            tags=[
                "a",
                "abbr",
                "acronym",
                "b",
                "blockquote",
                "code",
                "em",
                "i",
                "li",
                "ol",
                "ul",
                "p",
                "pre",
                "strong",
                "table",
                "thead",
                "tbody",
                "tr",
                "th",
                "td",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "hr",
                "br",
                "span",
                "img",
            ],
            attributes={
                "*": ["class"],
                "a": ["href", "title", "rel"],
                "abbr": ["title"],
                "acronym": ["title"],
                "code": ["class"],
                "pre": ["class"],
                "span": ["class", "style"],
                "p": ["class", "style"],
                "table": ["class", "style"],
                "td": ["class", "style"],
                "th": ["class", "style"],
                "img": ["src", "alt", "title", "width", "height", "loading", "decoding", "class"],
            },
            protocols=["http", "https", "mailto", "data"],
            strip=True,
            css_sanitizer=_css_sanitizer,
        )

        async def _render(name: str, **ctx: Any) -> HTMLResponse:
            tpl = env.get_template(name)
            html = await tpl.render_async(**ctx)
            return HTMLResponse(html)

        def _parse_fts_query(
            raw: str, scope_preference: str | None = None
        ) -> tuple[str, str, str, list[dict[str, str]]]:
            """Return (fts_expression, like_pattern) from a user query.
            Supports subject:foo and body:"multi word" tokens; otherwise defaults to subject/body OR.
            """
            raw = (raw or "").strip()
            if not raw:
                return "", "", "both", []
            scope_pref = scope_preference if scope_preference in {"subject", "body"} else "both"
            # tokens: key:"phrase" | "phrase" | key:word | word
            parts = re.findall(r"\w+:\"[^\"]+\"|\"[^\"]+\"|\w+:[^\s]+|[^\s]+", raw)
            exprs: list[str] = []
            like_terms: list[str] = []
            like_scope = scope_pref
            tokens: list[dict[str, str]] = []

            def _quote(s: str) -> str:
                return '"' + s.replace('"', '""') + '"'

            def _like_escape(term: str) -> str:
                return term.replace("!", "!!").replace("%", "!%").replace("_", "!_")

            for p in parts:
                key = None
                val = p
                if ":" in p and not p.startswith('"'):
                    maybe_key, maybe_val = p.split(":", 1)
                    if maybe_key in {"subject", "body"}:
                        key = maybe_key
                        val = maybe_val
                val = val.strip()
                val_inner = val[1:-1] if val.startswith('"') and val.endswith('"') and len(val) >= 2 else val

                # For LIKE pattern, we want literal matching of the user's term
                like_terms.append(_like_escape(val_inner))

                if key in {"subject", "body"}:
                    exprs.append(f"{key}:{_quote(val_inner)}")
                    tokens.append({"field": key, "value": val_inner})
                else:
                    if scope_pref == "subject":
                        exprs.append(f"subject:{_quote(val_inner)}")
                        tokens.append({"field": "subject", "value": val_inner})
                    elif scope_pref == "body":
                        exprs.append(f"body:{_quote(val_inner)}")
                        tokens.append({"field": "body", "value": val_inner})
                    else:
                        exprs.append(f"(subject:{_quote(val_inner)} OR body:{_quote(val_inner)})")
                        tokens.append({"field": "both", "value": val_inner})
            fts = " AND ".join(exprs) if exprs else ""
            like_pat = "%" + "%".join(like_terms) + "%" if like_terms else ""
            return fts, like_pat, like_scope, tokens

        @fastapi_app.get("/mail/api/locks", response_class=JSONResponse)
        async def mail_lock_status() -> JSONResponse:
            """Return metadata about active archive locks for observability."""

            settings_local = get_settings()
            payload = collect_lock_status(settings_local)
            return JSONResponse(payload)

        async def _build_unified_inbox_payload(
            *, limit: int = 500, include_projects: bool = True
        ) -> dict[str, Any]:
            """Fetch unified inbox data for HTML and JSON consumers."""

            safe_limit = max(1, min(int(limit), 1000))
            messages: list[dict[str, Any]] = []
            projects: list[dict[str, Any]] = []

            try:
                await ensure_schema()

                sibling_map: dict[int, dict[str, Any]] = {}
                if include_projects:
                    await refresh_project_sibling_suggestions()
                    sibling_map = await get_project_sibling_data()

                async with get_session() as session:
                    # Fetch recent messages with sender/project and computed recipient list
                    query = text(
                        """
                        SELECT
                            m.id,
                            m.subject,
                            m.body_md,
                            LENGTH(COALESCE(m.body_md, '')) AS body_length,
                            m.created_ts,
                            m.importance,
                            m.thread_id,
                            m.project_id AS message_project_id,
                            sender.name AS sender_name,
                            sender.project_id AS sender_project_id,
                            sp.human_key AS sender_project_name,
                            sp.slug AS sender_project_slug,
                            p.slug AS project_slug,
                            p.human_key AS project_name,
                            COALESCE(
                                (
                                    SELECT GROUP_CONCAT(name, ', ')
                                    FROM (
                                        SELECT DISTINCT recip2.name AS name
                                        FROM message_recipients mr2
                                        JOIN agents recip2 ON recip2.id = mr2.agent_id
                                        WHERE mr2.message_id = m.id
                                        ORDER BY name
                                    )
                                ),
                                ''
                            ) AS recipients
                        FROM messages m
                        JOIN agents sender ON m.sender_id = sender.id
                        LEFT JOIN projects sp ON sp.id = sender.project_id
                        JOIN projects p ON m.project_id = p.id
                        ORDER BY m.created_ts DESC
                        LIMIT :limit
                        """
                    )

                    rows = await session.execute(query, {"limit": safe_limit})

                    for r in rows.mappings().all():
                        body = r["body_md"] or ""
                        raw_body_length = r["body_length"]
                        body_length = int(raw_body_length) if raw_body_length is not None else len(body)
                        excerpt = body[:150].replace('#', '').replace('*', '').replace('`', '').strip()
                        if body_length > 150:
                            excerpt += "..."

                        created_ts = r["created_ts"]
                        if isinstance(created_ts, str):
                            created_dt = datetime.fromisoformat(created_ts.replace('Z', '+00:00'))
                        else:
                            created_dt = created_ts

                        if created_dt.tzinfo is None:
                            created_dt = created_dt.replace(tzinfo=timezone.utc)
                        else:
                            created_dt = created_dt.astimezone(timezone.utc)

                        now = datetime.now(timezone.utc)
                        delta = now - created_dt

                        if delta.days < 0 or (delta.days == 0 and delta.seconds < 0):
                            created_relative = "Just now"
                        elif delta.days > 365:
                            created_relative = f"{delta.days // 365}y ago"
                        elif delta.days > 30:
                            created_relative = f"{delta.days // 30}mo ago"
                        elif delta.days > 0:
                            created_relative = f"{delta.days}d ago"
                        elif delta.seconds > 3600:
                            created_relative = f"{delta.seconds // 3600}h ago"
                        elif delta.seconds > 60:
                            created_relative = f"{delta.seconds // 60}m ago"
                        else:
                            created_relative = "Just now"

                        sender_display, sender_meta = _http_sender_identity(
                            message_project_id=r["message_project_id"],
                            sender_name=r["sender_name"],
                            sender_project_id=r["sender_project_id"],
                            sender_project_human_key=r["sender_project_name"],
                            sender_project_slug=r["sender_project_slug"],
                        )
                        message_payload = {
                            "id": r["id"],
                            "subject": r["subject"] or "(No subject)",
                            "body_md": body,
                            "body_length": body_length,
                            "excerpt": excerpt,
                            "created_ts": str(r["created_ts"]),
                            "created_full": created_dt.strftime("%B %d, %Y at %I:%M %p"),
                            "created_relative": created_relative,
                            "importance": r["importance"] or "normal",
                            "thread_id": r["thread_id"],
                            "sender": sender_display,
                            "project_slug": r["project_slug"],
                            "project_name": r["project_name"],
                            "recipients": ", ".join(
                                part.strip() for part in (r["recipients"] or "").split(",") if part.strip()
                            ),
                            "read": False,
                        }
                        message_payload.update(sender_meta)
                        messages.append(message_payload)

                    if include_projects:
                        rows = await session.execute(
                            text("SELECT id, slug, human_key, created_at, archived_at FROM projects ORDER BY created_at DESC")
                        )
                        for r in rows.fetchall():
                            project_id = int(r[0])
                            siblings = sibling_map.get(project_id, {"confirmed": [], "suggested": []})
                            projects.append(
                                {
                                    "id": project_id,
                                    "slug": r[1],
                                    "human_key": r[2],
                                    "created_at": str(r[3]),
                                    "archived_at": str(r[4]) if r[4] else None,
                                    "confirmed_siblings": siblings.get("confirmed", []),
                                    "suggested_siblings": siblings.get("suggested", []),
                                }
                            )

            except Exception as exc:  # pragma: no cover - defensive logging
                logging.error("Error fetching unified inbox data", exc_info=True, extra={"error": str(exc)})

            return {"messages": messages, "projects": projects}

        @fastapi_app.get("/mail", response_class=HTMLResponse)
        async def mail_unified_inbox() -> HTMLResponse:
            """Unified inbox showing ALL messages across ALL projects (Gmail-style) + Projects below"""

            payload = await _build_unified_inbox_payload()
            return await _render(
                "mail_unified_inbox.html",
                messages=payload.get("messages", []),
                projects=payload.get("projects", []),
            )

        @fastapi_app.get("/mail/api/unified-inbox", response_class=JSONResponse)
        async def mail_unified_inbox_api(
            limit: int = 50000,
            include_projects: bool = False,
        ) -> JSONResponse:
            """JSON feed for the unified inbox view (used for background refresh)."""

            payload = await _build_unified_inbox_payload(limit=limit, include_projects=include_projects)
            if not include_projects:
                # Reduce payload size when polling for message updates only
                payload["projects"] = []
            return JSONResponse(payload)

        # ========== Live event stream ==========
        # The `events` table is the bus: every worker writes to it and every
        # stream polls it, so delivery does not depend on which gunicorn worker
        # handled the write.
        event_poll_seconds = 0.5
        event_heartbeat_seconds = 15.0
        event_batch_size = 500

        def _events_filter(
            project_id: int | None, agent: str | None, types: list[str]
        ) -> tuple[str, dict[str, Any]]:
            clauses: list[str] = []
            params: dict[str, Any] = {}
            if project_id is not None:
                clauses.append("e.project_id = :pid")
                params["pid"] = project_id
            if types:
                clauses.append("e.type IN (" + ", ".join(f":t{i}" for i in range(len(types))) + ")")
                params.update({f"t{i}": t for i, t in enumerate(types)})
            if agent:
                # An agent is involved when it acted, was addressed, or sent the message being read/acked.
                clauses.append(
                    "(e.actor = :agent"
                    " OR json_extract(e.payload, '$.sender') = :agent"
                    " OR EXISTS (SELECT 1 FROM json_each(e.payload, '$.to') WHERE value = :agent)"
                    " OR EXISTS (SELECT 1 FROM json_each(e.payload, '$.cc') WHERE value = :agent)"
                    " OR EXISTS (SELECT 1 FROM json_each(e.payload, '$.bcc') WHERE value = :agent))"
                )
                params["agent"] = agent
            return "".join(f" AND {c}" for c in clauses), params

        def _format_sse_event(row: Any) -> str:
            ts = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            payload = row[5]
            data = {
                "id": int(row[0]),
                "ts": ts.replace(tzinfo=timezone.utc).isoformat(),
                "project": row[2],
                "type": row[3],
                "actor": row[4],
                "payload": json.loads(payload) if isinstance(payload, str) else payload,
            }
            return f"id: {data['id']}\nevent: {data['type']}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"

        async def _add_read_events(session: AsyncSession, agent_name: str, message_ids: list[int]) -> None:
            """Queue a ``message.read`` event per newly read message on the caller's transaction."""
            for start in range(0, len(message_ids), event_batch_size):
                chunk = message_ids[start : start + event_batch_size]
                placeholders = ", ".join(f":m{i}" for i in range(len(chunk)))
                rows = (
                    await session.execute(
                        text(
                            "SELECT m.id, m.project_id, m.thread_id, m.subject, a.name "
                            f"FROM messages m JOIN agents a ON a.id = m.sender_id WHERE m.id IN ({placeholders})"
                        ),
                        {f"m{i}": mid for i, mid in enumerate(chunk)},
                    )
                ).fetchall()
                session.add_all(
                    Event(
                        project_id=int(r[1]),
                        type="message.read",
                        actor=agent_name,
                        payload={"message_id": int(r[0]), "thread_id": r[2], "subject": r[3], "sender": r[4]},
                    )
                    for r in rows
                )

        @fastapi_app.get("/mail/api/events")
        async def mail_events_stream(
            request: Request,
            project: str | None = None,
            agent: str | None = None,
            types: str | None = None,
            since: int | None = None,
            tail: int = 0,
            limit: int | None = None,
            wait_seconds: float | None = Query(None, alias="timeout"),
        ) -> StreamingResponse:
            """Server-Sent Events stream of the activity log.

            Resumes after the standard ``Last-Event-ID`` header or ``?since=<id>``;
            otherwise starts at the current end of the log, first replaying the last
            ``tail`` matching events. ``limit`` / ``timeout`` close the stream after
            that many events / seconds, so clients that cannot hold a connection
            open (hooks, cron) can long-poll the same endpoint.
            """
            await ensure_schema()
            project_id: int | None = None
            if project:
                try:
                    project_id = (await _get_project_by_identifier(project)).id
                except (ToolExecutionError, NoResultFound) as exc:
                    raise HTTPException(status_code=404, detail="Project not found") from exc
            type_list = [t.strip() for t in (types or "").split(",") if t.strip()]
            where, params = _events_filter(project_id, agent, type_list)
            select_sql = (
                "SELECT e.id, e.ts, p.slug, e.type, e.actor, e.payload "
                "FROM events e JOIN projects p ON p.id = e.project_id WHERE e.id > :cursor"
            )

            last_event_id = request.headers.get("last-event-id", "").strip()
            if last_event_id.isdigit():
                cursor = int(last_event_id)
            elif since is not None:
                cursor = max(since, 0)
            else:
                async with get_session() as session:
                    if tail > 0:
                        # Start just before the tail-th most recent matching event.
                        row = (
                            await session.execute(
                                text(
                                    "SELECT MIN(id) - 1 FROM (SELECT e.id FROM events e WHERE 1 = 1"
                                    f"{where} ORDER BY e.id DESC LIMIT :tail)"
                                ),
                                {**params, "tail": tail},
                            )
                        ).fetchone()
                        cursor = int(row[0]) if row and row[0] is not None else 0
                    else:
                        cursor = int((await session.execute(text("SELECT COALESCE(MAX(id), 0) FROM events"))).scalar_one())

            async def _stream() -> Any:
                position = cursor
                sent = 0
                loop = asyncio.get_running_loop()
                deadline = loop.time() + wait_seconds if wait_seconds is not None else None
                last_write = loop.time()
                yield "retry: 3000\n\n"
                while True:
                    if await request.is_disconnected():
                        return
                    async with get_session() as session:
                        rows = (
                            await session.execute(
                                text(f"{select_sql}{where} ORDER BY e.id LIMIT :batch"),
                                {**params, "cursor": position, "batch": event_batch_size},
                            )
                        ).fetchall()
                    for row in rows:
                        position = int(row[0])
                        yield _format_sse_event(row)
                        sent += 1
                        if limit is not None and sent >= limit:
                            return
                    now = loop.time()
                    if rows:
                        last_write = now
                        if len(rows) == event_batch_size:
                            continue
                    if deadline is not None and now >= deadline:
                        return
                    if now - last_write >= event_heartbeat_seconds:
                        yield ": heartbeat\n\n"
                        last_write = now
                    wait = event_poll_seconds if deadline is None else min(event_poll_seconds, deadline - now)
                    await asyncio.sleep(wait)

            return StreamingResponse(
                _stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        # ========== Presence ==========

        @fastapi_app.post("/mail/api/presence")
        async def mail_presence_heartbeat(request: Request) -> JSONResponse:
            """Heartbeat from an agent's push-delivery hooks (scripts/hooks/mail_wake.py).

            JSON body: project, agent, and optionally state (busy|idle|offline),
            listener (true while its mail listener runs, false when it stops),
            machine, client and session.
            """
            try:
                body = await request.json()
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Body must be JSON") from exc
            if not isinstance(body, dict) or not body.get("project") or not body.get("agent"):
                raise HTTPException(status_code=400, detail="project and agent are required")
            listener = body.get("listener")
            try:
                project = await _get_project_by_identifier(str(body["project"]))
                agent = await _get_agent(project, str(body["agent"]))
            except (ToolExecutionError, NoResultFound) as exc:
                raise HTTPException(status_code=404, detail="Unknown project or agent") from exc
            try:
                view = await _record_presence(
                    project,
                    agent,
                    state=body.get("state"),
                    listener=listener if isinstance(listener, bool) else None,
                    machine=str(body.get("machine") or ""),
                    client=str(body.get("client") or ""),
                    session_id=str(body.get("session") or ""),
                    client_version=_client_script_version(request) or "",
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            # The script tells its agent when a newer client is available.
            return JSONResponse(view | {"client_latest": served_client_version()})

        @fastapi_app.get("/mail/api/presence")
        async def mail_presence_list(project: str, include_offline: bool = False) -> JSONResponse:
            """Who is online in a project (the who_is_online tool, for the web UI)."""
            try:
                resolved = await _get_project_by_identifier(project)
            except (ToolExecutionError, NoResultFound) as exc:
                raise HTTPException(status_code=404, detail="Project not found") from exc
            agents = await _presence_listing(resolved, include_offline=include_offline)
            return JSONResponse({"project": resolved.slug, "agents": agents})

        # ========== File attachments (cross-machine) ==========

        @fastapi_app.post("/mail/api/attachments")
        async def mail_attachment_upload(request: Request, project: str, agent: str, filename: str) -> JSONResponse:
            """Upload one file (raw request body) for attaching to messages by id.

            Done by scripts/hooks/agent_mail_files.py, so file bytes never pass through a model.
            Returns {id, name, size, sha256, media_type}; pass the id in send_message/reply_message
            `attachment_ids`.
            """
            try:
                resolved = await _get_project_by_identifier(project)
                uploader = await _get_agent(resolved, agent)
            except (ToolExecutionError, NoResultFound) as exc:
                raise HTTPException(status_code=404, detail="Unknown project or agent") from exc
            declared = request.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > settings.storage.attachment_max_bytes:
                raise HTTPException(status_code=413, detail="Attachment too large (ATTACHMENT_MAX_BYTES)")
            try:
                meta = await _store_file_attachment(resolved, uploader.name, filename, request.stream(), get_settings())
            except AttachmentTooLargeError as exc:
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            return JSONResponse(meta, status_code=201)

        @fastapi_app.get("/mail/api/attachments/{attachment_id}")
        async def mail_attachment_download(attachment_id: str) -> FileResponse:
            """Download an uploaded file, always as a download (never rendered), with its SHA-256 in a header."""
            row = await _get_file_attachment(attachment_id)
            path = _blob_path(get_settings(), row.sha256) if row else None
            if row is None or path is None or not path.is_file():
                raise HTTPException(status_code=404, detail="Attachment not found")
            return FileResponse(
                path,
                media_type="application/octet-stream",
                filename=row.filename,
                headers={"X-Content-SHA256": row.sha256, "X-Content-Type-Options": "nosniff"},
            )

        # ========== Browser login (see UI_SESSION_COOKIE) ==========
        def _safe_next(target: str | None) -> str:
            # Only same-site /mail paths: never "//host" or "/\\host", which browsers treat as other origins.
            if not target or not _is_mail_ui_path(target) or "\\" in target:
                return "/mail"
            return target

        sso_only = settings.http.ui_sso_only
        github_login_enabled = (
            bool(settings.http.github_client_id and settings.http.github_client_secret) and not sso_only
        )
        token_login_enabled = settings.http.ui_token_login and not sso_only

        async def _login_page(target: str, error: str | None, status_code: int = 200) -> HTMLResponse:
            response = await _render(
                "mail_login.html",
                next=target,
                error=error,
                oidc_enabled=_oidc_enabled(),
                oidc_name=settings.http.oidc_display_name,
                github_enabled=github_login_enabled,
                password_login=not sso_only and await ui_auth.has_users(),
                token_login=token_login_enabled,
            )
            response.status_code = status_code
            return response

        async def _start_session(response: Response, request: Request, user: str, method: str) -> Response:
            """The one place a session begins and its cookie is set (lfr-tunnel #1655: copies drift)."""
            client_ip = request.client.host if request.client else ""
            session_token = await ui_auth.create_session(
                user, method, ip=client_ip, user_agent=request.headers.get("user-agent", "")
            )
            response.set_cookie(
                UI_SESSION_COOKIE,
                session_token,
                max_age=UI_SESSION_MAX_AGE_SECONDS,
                path="/mail",
                httponly=True,
                # Lax still withholds the cookie from cross-site POSTs (every state change here is a POST)
                # while letting a link to a /mail page from elsewhere open signed in.
                samesite="lax",
                secure=request.url.scheme == "https",
            )
            response.delete_cookie(UI_OAUTH_STATE_COOKIE, path=UI_AUTH_PREFIX.rstrip("/"))
            response.delete_cookie(UI_MFA_COOKIE, path=UI_AUTH_PREFIX.rstrip("/"))
            await audit.record("auth.signin", actor=user, details=f"method={method}", request=request)
            return response

        async def _actor(request: Request) -> str:
            """Who is acting, for the audit log: the signed-in person, a token's owner, or how they got in."""
            owner = getattr(request.state, "agent_mail_owner", None)
            if owner:
                return str(owner)
            if user := await _ui_session_user(request):
                return user
            if request.headers.get("Authorization", "").startswith("Bearer "):
                return "bearer-token"
            return "unauthenticated" if settings.http.bearer_token else "open-server"

        async def _signed_in(request: Request, target: str, user: str, method: str) -> Response:
            return await _start_session(RedirectResponse(target, status_code=status.HTTP_303_SEE_OTHER), request, user, method)

        async def _signin_failed(request: Request, actor: str, method: str, reason: str, *, locked: bool = False) -> None:
            await audit.record("auth.signin_failed", actor=actor, details=f"method={method}; {reason}", request=request)
            if locked:
                await audit.record("auth.locked", actor=actor, target_type="user", target_id=actor, request=request)
            if (ip := ip_guard.client_ip(request)) is not None:
                await ip_guard.record_failure(ip, "signin")

        async def _signin_paused(request: Request, target: str) -> Response | None:
            """A 429 sign-in page when this IP has failed too often recently (ip_guard.py), else None."""
            ip = ip_guard.client_ip(request)
            if ip is None or not await ip_guard.signin_blocked(ip):
                return None
            await audit.record("auth.signin_throttled", actor="anonymous", target_type="ip", target_id=ip, request=request)
            minutes = int(ip_guard.SIGNIN_WINDOW.total_seconds() // 60)
            page = await _login_page(
                target, f"Too many failed sign-ins from your network. Try again in up to {minutes} minutes.",
                status.HTTP_429_TOO_MANY_REQUESTS,
            )
            page.headers["Retry-After"] = str(int(ip_guard.SIGNIN_WINDOW.total_seconds()))
            return page

        # ----- Username + password + authenticator code (ui_auth.py) -----
        # Between the password and the code, a short-lived signed cookie remembers
        # which account passed the first factor ("mfa": enter a code, "enroll":
        # set up the authenticator). It grants nothing else.

        def _pending_cookie(user_id: int, stage: str) -> str:
            expires = int(time.time()) + MFA_PENDING_MAX_AGE_SECONDS
            sig = _ui_sign(settings.http.bearer_token or "", f"mfa:{user_id}:{stage}:{expires}")
            return f"{expires}.{user_id}.{stage}.{sig}"

        def _read_pending_cookie(request: Request, stage: str) -> int | None:
            parts = request.cookies.get(UI_MFA_COOKIE, "").split(".")
            if len(parts) != 4 or not settings.http.bearer_token:
                return None
            expires, user_id, got_stage, sig = parts
            if got_stage != stage or not expires.isdigit() or int(expires) < time.time() or not user_id.isdigit():
                return None
            expected = _ui_sign(settings.http.bearer_token, f"mfa:{user_id}:{got_stage}:{expires}")
            return int(user_id) if hmac.compare_digest(sig, expected) else None

        def _with_pending(response: Response, request: Request, user_id: int, stage: str) -> Response:
            response.set_cookie(
                UI_MFA_COOKIE,
                _pending_cookie(user_id, stage),
                max_age=MFA_PENDING_MAX_AGE_SECONDS,
                path=UI_AUTH_PREFIX.rstrip("/"),
                httponly=True,
                samesite="strict",
                secure=request.url.scheme == "https",
            )
            return response

        async def _mfa_page(mode: str, target: str, error: str | None = None, status_code: int = 200, **ctx: Any) -> HTMLResponse:
            response = await _render("mail_login_mfa.html", mode=mode, next=target, error=error, **ctx)
            response.status_code = status_code
            return response

        def _expired(target: str) -> Response:
            return RedirectResponse(f"{UI_LOGIN_PATH}?{urlencode({'next': target})}", status_code=status.HTTP_303_SEE_OTHER)

        async def _form(request: Request) -> dict[str, str]:
            parsed = parse_qs((await request.body()).decode("utf-8", errors="replace"))
            return {key: values[0] for key, values in parsed.items()}

        @fastapi_app.post(f"{UI_AUTH_PREFIX}password")
        async def mail_password_sign_in(request: Request) -> Response:
            form = await _form(request)
            target = _safe_next(form.get("next"))
            if (paused := await _signin_paused(request, target)) is not None:
                return paused
            attempted = form.get("username", "").strip().lower()[:64]
            if sso_only:
                await _signin_failed(request, f"{ui_auth.SESSION_PREFIX}{attempted}", "password", "SSO-only mode")
                return await _login_page(target, "Password sign-in is turned off on this server.", status.HTTP_403_FORBIDDEN)
            try:
                user = await ui_auth.check_password(attempted, form.get("password", ""))
            except ui_auth.AuthError as exc:
                await _signin_failed(request, f"{ui_auth.SESSION_PREFIX}{attempted}", "password", str(exc), locked=exc.locked)
                return await _login_page(target, str(exc), status.HTTP_401_UNAUTHORIZED)
            assert user.id is not None
            stage = "mfa" if user.totp_secret else "enroll"
            path = f"{UI_AUTH_PREFIX}mfa" if stage == "mfa" else f"{UI_AUTH_PREFIX}mfa/setup"
            redirect = RedirectResponse(f"{path}?{urlencode({'next': target})}", status_code=status.HTTP_303_SEE_OTHER)
            return _with_pending(redirect, request, user.id, stage)

        @fastapi_app.get(f"{UI_AUTH_PREFIX}mfa", response_class=HTMLResponse)
        async def mail_mfa_form(request: Request, next_path: str | None = Query(None, alias="next")) -> Response:
            target = _safe_next(next_path)
            if _read_pending_cookie(request, "mfa") is None:
                return _expired(target)
            return await _mfa_page("code", target)

        @fastapi_app.post(f"{UI_AUTH_PREFIX}mfa")
        async def mail_mfa_check(request: Request) -> Response:
            form = await _form(request)
            target = _safe_next(form.get("next"))
            user_id = _read_pending_cookie(request, "mfa")
            if user_id is None:
                return _expired(target)
            if (paused := await _signin_paused(request, target)) is not None:
                return paused
            try:
                user = await ui_auth.check_second_factor(user_id, form.get("code", ""))
            except ui_auth.AuthError as exc:
                pending = await ui_auth.get_user_by_id(user_id)
                actor = f"{ui_auth.SESSION_PREFIX}{pending.username}" if pending else f"user-id:{user_id}"
                await _signin_failed(request, actor, "password", f"second factor: {exc}", locked=exc.locked)
                return await _mfa_page("code", target, str(exc), status.HTTP_401_UNAUTHORIZED)
            return await _signed_in(request, target, f"{ui_auth.SESSION_PREFIX}{user.username}", "password")

        @fastapi_app.get(f"{UI_AUTH_PREFIX}mfa/setup", response_class=HTMLResponse)
        async def mail_mfa_setup_form(request: Request, next_path: str | None = Query(None, alias="next")) -> Response:
            target = _safe_next(next_path)
            user_id = _read_pending_cookie(request, "enroll")
            if user_id is None:
                return _expired(target)
            try:
                user, secret = await ui_auth.begin_enrollment(user_id)
            except ui_auth.AuthError:
                return _expired(target)
            return await _mfa_page(
                "setup", target, secret=secret, username=user.username, otpauth=ui_auth.otpauth_uri(secret, user.username)
            )

        @fastapi_app.post(f"{UI_AUTH_PREFIX}mfa/setup")
        async def mail_mfa_setup_confirm(request: Request) -> Response:
            form = await _form(request)
            target = _safe_next(form.get("next"))
            user_id = _read_pending_cookie(request, "enroll")
            if user_id is None:
                return _expired(target)
            if (paused := await _signin_paused(request, target)) is not None:
                return paused
            try:
                user, codes = await ui_auth.complete_enrollment(user_id, form.get("code", ""))
            except ui_auth.AuthError as exc:
                user, secret = await ui_auth.begin_enrollment(user_id)
                who = f"{ui_auth.SESSION_PREFIX}{user.username}"
                await _signin_failed(request, who, "password", f"authenticator setup: {exc}")
                return await _mfa_page(
                    "setup", target, str(exc), status.HTTP_401_UNAUTHORIZED,
                    secret=secret, username=user.username, otpauth=ui_auth.otpauth_uri(secret, user.username),
                )
            # Show the recovery codes once; the session starts with this response.
            who = f"{ui_auth.SESSION_PREFIX}{user.username}"
            await audit.record("auth.mfa_enrolled", actor=who, target_type="user", target_id=who, request=request)
            return await _start_session(await _mfa_page("recovery", target, recovery_codes=codes), request, who, "password")

        # ----- Personal access tokens: `mail_wake.py login` and their management -----
        # The CLI opens /mail/auth/cli in the browser with a random state and the port of a
        # listener on 127.0.0.1. After the person approves, the token goes back to that listener
        # by a redirect (the loopback flow for native apps, RFC 8252), or is shown for pasting
        # when there is no listener (--no-browser, e.g. over SSH).

        async def _session_user(request: Request) -> str | None:
            return await _ui_session_user(request)

        def _cli_params(port: str, state: str, name: str) -> tuple[int | None, str, str] | None:
            if port and not (port.isdigit() and 1024 <= int(port) <= 65535):
                return None
            if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", state or ""):
                return None
            return (int(port) if port else None), state, (name or "").strip()[:128] or "unnamed"

        @fastapi_app.get(f"{UI_AUTH_PREFIX}cli", response_class=HTMLResponse)
        async def mail_cli_login_form(request: Request, port: str = "", state: str = "", name: str = "") -> Response:
            params = _cli_params(port, state, name)
            if params is None:
                raise HTTPException(status_code=400, detail="Invalid sign-in request from the command line")
            user = await _session_user(request)
            if user is None:  # sign in first, then come straight back here
                here = f"{UI_AUTH_PREFIX}cli?{request.url.query}"
                return RedirectResponse(f"{UI_LOGIN_PATH}?{urlencode({'next': here})}", status_code=status.HTTP_303_SEE_OTHER)
            return await _render(
                "mail_tokens.html", mode="consent", user=user, port=params[0] or "", state=params[1], token_name=params[2],
                lifetime_days=ui_auth.API_TOKEN_LIFETIME.days,
            )

        @fastapi_app.post(f"{UI_AUTH_PREFIX}cli")
        async def mail_cli_login_approve(request: Request) -> Response:
            # Cross-site POSTs carry no session cookie (SameSite=Lax), so only this site's own
            # consent form can get here signed in.
            user = await _session_user(request)
            if user is None:
                return RedirectResponse(UI_LOGIN_PATH, status_code=status.HTTP_303_SEE_OTHER)
            form = await _form(request)
            params = _cli_params(form.get("port", ""), form.get("state", ""), form.get("name", ""))
            if params is None:
                raise HTTPException(status_code=400, detail="Invalid sign-in request from the command line")
            port, state, name = params
            token, row = await ui_auth.create_api_token(user, name)
            await audit.record(
                "token.created", actor=user, target_type="api_token", target_id=str(row.id),
                details=f"name={name}; prefix={row.prefix}", request=request,
            )
            if port is not None:
                return RedirectResponse(
                    f"http://127.0.0.1:{port}/callback?{urlencode({'state': state, 'token': token})}",
                    status_code=status.HTTP_303_SEE_OTHER,
                )
            return await _render("mail_tokens.html", mode="show", user=user, token=token, token_name=name)

        @fastapi_app.get("/mail/account/tokens", response_class=HTMLResponse)
        async def mail_tokens_page(request: Request) -> Response:
            current = await _ui_session(request)
            if current is None:  # e.g. signed in by localhost bypass: tokens and sessions belong to people
                return RedirectResponse(f"{UI_LOGIN_PATH}?next=/mail/account/tokens", status_code=status.HTTP_303_SEE_OTHER)
            tokens = await ui_auth.list_api_tokens(current.user)
            sessions = await ui_auth.list_sessions(current.user)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            return await _render(
                "mail_tokens.html", mode="list", user=current.user, tokens=tokens, sessions=sessions,
                current_session_id=current.id, now=now,
            )

        @fastapi_app.post("/mail/account/tokens/{token_id}/revoke")
        async def mail_token_revoke(request: Request, token_id: int) -> Response:
            user = await _session_user(request)
            if user is None or not await ui_auth.revoke_api_token(user, token_id):
                raise HTTPException(status_code=404, detail="Token not found")
            await audit.record("token.revoked", actor=user, target_type="api_token", target_id=str(token_id), request=request)
            return RedirectResponse("/mail/account/tokens", status_code=status.HTTP_303_SEE_OTHER)

        @fastapi_app.post("/mail/account/sessions/{session_id}/revoke")
        async def mail_session_revoke(request: Request, session_id: int) -> Response:
            user = await _session_user(request)
            if user is None or not await ui_auth.revoke_session(user, session_id):
                raise HTTPException(status_code=404, detail="Session not found")
            await audit.record("session.revoked", actor=user, target_type="session", target_id=str(session_id), request=request)
            return RedirectResponse("/mail/account/tokens", status_code=status.HTTP_303_SEE_OTHER)

        @fastapi_app.post("/mail/account/sessions/revoke-all")
        async def mail_sessions_revoke_all(request: Request) -> Response:
            user = await _session_user(request)
            if user is None:
                raise HTTPException(status_code=404, detail="Not signed in")
            count = await ui_auth.revoke_all_sessions(user)
            await audit.record("session.revoked_all", actor=user, details=f"sessions={count}", request=request)
            response = RedirectResponse(UI_LOGIN_PATH, status_code=status.HTTP_303_SEE_OTHER)
            response.delete_cookie(UI_SESSION_COOKIE, path="/mail")
            return response

        @fastapi_app.get(UI_LOGIN_PATH, response_class=HTMLResponse)
        async def mail_login_form(next_path: str | None = Query(None, alias="next")) -> Response:
            if not settings.http.bearer_token:
                return RedirectResponse(_safe_next(next_path), status_code=status.HTTP_303_SEE_OTHER)
            return await _login_page(_safe_next(next_path), None)

        @fastapi_app.post(UI_LOGIN_PATH)
        async def mail_login(request: Request) -> Response:
            """Bearer-token sign-in (admin fallback; HTTP_UI_TOKEN_LOGIN=false turns it off)."""
            form = parse_qs((await request.body()).decode("utf-8", errors="replace"))
            target = _safe_next((form.get("next") or [None])[0])
            token = settings.http.bearer_token or ""
            if (paused := await _signin_paused(request, target)) is not None:
                return paused
            if not token_login_enabled:
                await _signin_failed(request, TOKEN_LOGIN_USER, "token", "token sign-in is turned off")
                return await _login_page(target, "Token sign-in is turned off on this server.", status.HTTP_403_FORBIDDEN)
            submitted = (form.get("token") or [""])[0].strip()
            if not token or not hmac.compare_digest(submitted.encode(), token.encode()):
                await _signin_failed(request, TOKEN_LOGIN_USER, "token", "wrong token")
                return await _login_page(target, "That token is not valid.", status.HTTP_401_UNAUTHORIZED)
            return await _signed_in(request, target, TOKEN_LOGIN_USER, "token")

        # ----- Sign in with GitHub (OAuth web flow) -----

        def _oauth_state_cookie(state: str, target: str) -> str:
            data = base64.urlsafe_b64encode(json.dumps({"s": state, "n": target}).encode()).decode().rstrip("=")
            return f"{data}.{_ui_sign(settings.http.bearer_token or '', f'oauth:{data}')}"

        def _read_oauth_state_cookie(raw: str) -> tuple[str, str] | None:
            data, _, signature = raw.partition(".")
            if not data or not hmac.compare_digest(signature, _ui_sign(settings.http.bearer_token or "", f"oauth:{data}")):
                return None
            with contextlib.suppress(ValueError):
                decoded = json.loads(base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)))
                return str(decoded["s"]), _safe_next(decoded.get("n"))
            return None

        def _github_callback_url(request: Request) -> str:
            return f"{str(request.base_url).rstrip('/')}{UI_AUTH_PREFIX}github/callback"

        @fastapi_app.get(f"{UI_AUTH_PREFIX}github")
        async def mail_github_start(request: Request, next_path: str | None = Query(None, alias="next")) -> Response:
            if not github_login_enabled or not settings.http.bearer_token:
                raise HTTPException(status_code=404, detail="GitHub sign-in is not configured")
            state = secrets.token_urlsafe(24)
            query = urlencode({
                "client_id": settings.http.github_client_id,
                "redirect_uri": _github_callback_url(request),
                "state": state,
                "allow_signup": "false",
            })
            response = RedirectResponse(f"https://github.com/login/oauth/authorize?{query}", status_code=status.HTTP_303_SEE_OTHER)
            response.set_cookie(
                UI_OAUTH_STATE_COOKIE,
                _oauth_state_cookie(state, _safe_next(next_path)),
                max_age=UI_OAUTH_STATE_MAX_AGE_SECONDS,
                path=UI_AUTH_PREFIX.rstrip("/"),
                httponly=True,
                samesite="lax",  # sent on GitHub's top-level redirect back to us
                secure=request.url.scheme == "https",
            )
            return response

        @fastapi_app.get(f"{UI_AUTH_PREFIX}github/callback")
        async def mail_github_callback(request: Request, code: str = "", state: str = "", error: str = "") -> Response:
            if not github_login_enabled or not settings.http.bearer_token:
                raise HTTPException(status_code=404, detail="GitHub sign-in is not configured")
            remembered = _read_oauth_state_cookie(request.cookies.get(UI_OAUTH_STATE_COOKIE, ""))
            # The state must match the one we sent from this browser: otherwise anyone could
            # hand a victim a callback link that signs them in as someone else.
            if remembered is None or not state or not hmac.compare_digest(state, remembered[0]):
                return await _login_page("/mail", "Sign-in expired or was not started here. Please try again.", status.HTTP_400_BAD_REQUEST)
            target = remembered[1]
            if (paused := await _signin_paused(request, target)) is not None:
                return paused
            if error or not code:
                return await _login_page(target, "GitHub sign-in was cancelled.", status.HTTP_400_BAD_REQUEST)
            try:
                login = await _github_login_for_code(
                    code, settings.http.github_client_id, settings.http.github_client_secret, _github_callback_url(request)
                )
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                structlog.get_logger("ui").warning("github_sign_in_failed", error=repr(exc))
                await _signin_failed(request, "github:?", "github", f"code exchange failed: {exc!r}"[:300])
                return await _login_page(target, "GitHub sign-in failed. Please try again.", status.HTTP_502_BAD_GATEWAY)
            if login.lower() not in settings.http.github_allowed_users:
                structlog.get_logger("ui").warning("github_sign_in_refused", login=login)
                await _signin_failed(request, login.lower(), "github", "not on GITHUB_ALLOWED_USERS")
                return await _login_page(
                    target, f"GitHub user {login} is not allowed to use this server.", status.HTTP_403_FORBIDDEN
                )
            return await _signed_in(request, target, login.lower(), "github")

        # ----- Company sign-in (OpenID Connect; oidc.py) -----
        # The state, nonce, PKCE verifier and next page ride in a short-lived signed cookie,
        # like GitHub's state: the callback must come back to the browser that started it.

        def _pack(payload: dict[str, str]) -> str:
            data = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
            return f"{data}.{_ui_sign(settings.http.bearer_token or '', f'oidc:{data}')}"

        def _unpack(raw: str) -> dict[str, str] | None:
            data, _, signature = raw.partition(".")
            if not data or not hmac.compare_digest(signature, _ui_sign(settings.http.bearer_token or "", f"oidc:{data}")):
                return None
            with contextlib.suppress(ValueError):
                decoded = json.loads(base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)))
                return decoded if isinstance(decoded, dict) else None
            return None

        def _oidc_callback_url(request: Request) -> str:
            return f"{str(request.base_url).rstrip('/')}{UI_AUTH_PREFIX}oidc/callback"

        @fastapi_app.get(f"{UI_AUTH_PREFIX}oidc")
        async def mail_oidc_start(request: Request, next_path: str | None = Query(None, alias="next")) -> Response:
            if not _oidc_enabled() or not settings.http.bearer_token:
                raise HTTPException(status_code=404, detail="Company sign-in is not configured")
            try:
                started = await oidc.begin(
                    issuer=settings.http.oidc_issuer, client_id=settings.http.oidc_client_id,
                    redirect_uri=_oidc_callback_url(request), scopes=settings.http.oidc_scopes,
                )
            except (httpx.HTTPError, oidc.OidcError, KeyError, ValueError) as exc:
                structlog.get_logger("ui").warning("oidc_discovery_failed", error=repr(exc))
                return await _login_page(_safe_next(next_path), "Company sign-in is unavailable right now.", status.HTTP_502_BAD_GATEWAY)
            response = RedirectResponse(started.authorize_url, status_code=status.HTTP_303_SEE_OTHER)
            response.set_cookie(
                UI_OIDC_STATE_COOKIE,
                _pack({"s": started.state, "nonce": started.nonce, "v": started.code_verifier, "n": _safe_next(next_path)}),
                max_age=UI_OAUTH_STATE_MAX_AGE_SECONDS,
                path=UI_AUTH_PREFIX.rstrip("/"),
                httponly=True,
                samesite="lax",  # sent on the provider's top-level redirect back
                secure=request.url.scheme == "https",
            )
            return response

        @fastapi_app.get(f"{UI_AUTH_PREFIX}oidc/callback")
        async def mail_oidc_callback(request: Request, code: str = "", state: str = "", error: str = "") -> Response:
            if not _oidc_enabled() or not settings.http.bearer_token:
                raise HTTPException(status_code=404, detail="Company sign-in is not configured")
            remembered = _unpack(request.cookies.get(UI_OIDC_STATE_COOKIE, ""))
            if remembered is None or not state or not hmac.compare_digest(state, str(remembered.get("s", ""))):
                return await _login_page("/mail", "Sign-in expired or was not started here. Please try again.", status.HTTP_400_BAD_REQUEST)
            target = _safe_next(remembered.get("n"))
            if (paused := await _signin_paused(request, target)) is not None:
                return paused
            if error or not code:
                return await _login_page(target, "Company sign-in was cancelled.", status.HTTP_400_BAD_REQUEST)
            try:
                claims = await oidc.finish(
                    issuer=settings.http.oidc_issuer, client_id=settings.http.oidc_client_id,
                    client_secret=settings.http.oidc_client_secret, redirect_uri=_oidc_callback_url(request),
                    code=code, nonce=str(remembered.get("nonce", "")), code_verifier=str(remembered.get("v", "")),
                )
            except (httpx.HTTPError, oidc.OidcError, KeyError, ValueError) as exc:
                await _signin_failed(request, f"{OIDC_SESSION_PREFIX}?", "sso", str(exc)[:300])
                response = await _login_page(target, "Company sign-in failed. Please try again.", status.HTTP_502_BAD_GATEWAY)
                response.delete_cookie(UI_OIDC_STATE_COOKIE, path=UI_AUTH_PREFIX.rstrip("/"))
                return response
            email = claims["email"]
            if not _oidc_email_allowed(email):
                await _signin_failed(request, f"{OIDC_SESSION_PREFIX}{email}", "sso", "not in OIDC_ALLOWED_DOMAINS / OIDC_ALLOWED_EMAILS")
                return await _login_page(target, f"{email} is not allowed to use this server.", status.HTTP_403_FORBIDDEN)
            response = await _signed_in(request, target, f"{OIDC_SESSION_PREFIX}{email}", "sso")
            response.delete_cookie(UI_OIDC_STATE_COOKIE, path=UI_AUTH_PREFIX.rstrip("/"))
            return response

        @fastapi_app.post("/mail/logout")
        async def mail_logout(request: Request) -> Response:
            ended = await ui_auth.revoke_session_token(request.cookies.get(UI_SESSION_COOKIE, ""))
            if ended is not None:
                await audit.record("auth.signout", actor=ended.user, target_type="session", target_id=str(ended.id), request=request)
            response = RedirectResponse(UI_LOGIN_PATH, status_code=status.HTTP_303_SEE_OTHER)
            response.delete_cookie(UI_SESSION_COOKIE, path="/mail")
            return response

        @fastapi_app.get("/mail/audit", response_class=HTMLResponse)
        async def mail_audit_page(request: Request, actor: str = "", action: str = "", limit: int = 200) -> Response:
            """Security audit log (sign-ins, sessions, tokens, accounts, destructive UI actions)."""
            entries = await audit.recent(limit=limit, actor=actor.strip(), action_prefix=action.strip())
            return await _render("mail_audit.html", entries=entries, actor=actor, action=action, limit=limit)

        @fastapi_app.get("/mail/live", response_class=HTMLResponse)
        async def mail_live() -> HTMLResponse:
            """Live view of inter-agent activity, fed by /mail/api/events."""
            await ensure_schema()
            async with get_session() as session:
                rows = (
                    await session.execute(text("SELECT slug, human_key FROM projects ORDER BY human_key"))
                ).fetchall()
            return await _render("mail_live.html", projects=[{"slug": r[0], "human_key": r[1]} for r in rows])

        @fastapi_app.post("/mail/api/delete-messages", response_class=JSONResponse)
        async def delete_messages_api(request: Request) -> JSONResponse:
            """Permanently delete messages by ID (cross-project).

            Removes messages from the SQLite database AND deletes the
            corresponding markdown files from the Git archive.
            """
            await ensure_schema()

            try:
                request_body = await request.json()
                message_ids: list[int] = request_body.get("message_ids", [])

                if not message_ids:
                    raise HTTPException(status_code=400, detail="No message IDs provided")

                if len(message_ids) > 500:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Too many messages ({len(message_ids)}). Maximum is 500."
                    )

                deleted_count = 0
                messages_by_project: dict[str, list[tuple[Any, ...]]] = {}
                recip_map: dict[int, list[str]] = {}
                async with get_session() as session:
                    placeholders = ','.join([f':mid{i}' for i in range(len(message_ids))])
                    id_params: dict[str, Any] = {f"mid{i}": mid for i, mid in enumerate(message_ids)}

                    # Fetch message metadata for Git cleanup
                    rows = await session.execute(
                        text(
                            f"""
                            SELECT m.id, m.created_ts, m.subject, s.name AS sender_name,
                                   p.slug AS project_slug
                            FROM messages m
                            JOIN agents s ON s.id = m.sender_id
                            JOIN projects p ON p.id = m.project_id
                            WHERE m.id IN ({placeholders})
                            """
                        ),
                        id_params,
                    )
                    messages_to_delete = [tuple(row) for row in rows.fetchall()]

                    if not messages_to_delete:
                        return JSONResponse({"success": True, "deleted_count": 0})

                    # Collect recipients per message
                    recip_rows = await session.execute(
                        text(
                            f"""
                            SELECT mr.message_id, a.name
                            FROM message_recipients mr
                            JOIN agents a ON a.id = mr.agent_id
                            WHERE mr.message_id IN ({placeholders})
                            """
                        ),
                        id_params,
                    )
                    for rr in recip_rows.fetchall():
                        recip_map.setdefault(int(rr[0]), []).append(rr[1])

                    for mrow in messages_to_delete:
                        slug = str(mrow[4])
                        messages_by_project.setdefault(slug, []).append(mrow)

                    # Delete from SQLite
                    await session.execute(
                        text(f"DELETE FROM message_recipients WHERE message_id IN ({placeholders})"),
                        id_params,
                    )
                    del_result = await session.execute(
                        text(f"DELETE FROM messages WHERE id IN ({placeholders})"),
                        id_params,
                    )
                    deleted_count = int(getattr(del_result, "rowcount", 0) or 0)
                    await session.commit()

                settings = get_settings()
                total_git_files_removed = 0
                for project_slug, proj_msgs in messages_by_project.items():
                    try:
                        total_git_files_removed += await _delete_messages_from_archive(
                            settings=settings,
                            project_slug=project_slug,
                            messages_to_delete=proj_msgs,
                            recip_map=recip_map,
                            commit_message=f"delete: {len(proj_msgs)} message(s) via web UI\n",
                        )
                    except Exception as archive_exc:
                        logging.getLogger(__name__).warning(
                            "Git archive cleanup failed for project %s: %s",
                            project_slug,
                            archive_exc,
                        )

                await audit.record(
                    "messages.deleted", actor=await _actor(request), target_type="message",
                    target_id=",".join(map(str, message_ids))[:256], details=f"deleted={deleted_count}", request=request,
                )
                return JSONResponse({
                    "success": True,
                    "deleted_count": deleted_count,
                    "git_files_removed": total_git_files_removed,
                })

            except HTTPException:
                raise
            except Exception as exc:
                import traceback
                traceback.print_exc()
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to delete messages: {exc!s}"
                ) from exc

        # ---- Agent Retire/Unretire API ----

        @fastapi_app.post("/mail/api/retire-agent", response_class=JSONResponse)
        async def retire_agent_api(request: Request) -> JSONResponse:
            """Retire an agent (soft-delete). Preserves message history but stops new messages."""
            await ensure_schema()
            try:
                body = await request.json()
                agent_id: int | None = body.get("agent_id")
                if agent_id is None:
                    raise HTTPException(status_code=400, detail="agent_id is required")

                async with get_session() as session:
                    from .models import Agent
                    agent = await session.get(Agent, agent_id)
                    if not agent:
                        raise HTTPException(status_code=404, detail="Agent not found")
                    agent.retired_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    session.add(agent)
                    await session.commit()

                await audit.record("agent.retired", actor=await _actor(request), target_type="agent", target_id=str(agent_id), request=request)
                return JSONResponse({"success": True, "agent_id": agent_id, "status": "retired"})
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Failed to retire agent: {exc!s}") from exc

        @fastapi_app.post("/mail/api/unretire-agent", response_class=JSONResponse)
        async def unretire_agent_api(request: Request) -> JSONResponse:
            """Restore a retired agent back to active status."""
            await ensure_schema()
            try:
                body = await request.json()
                agent_id: int | None = body.get("agent_id")
                if agent_id is None:
                    raise HTTPException(status_code=400, detail="agent_id is required")

                async with get_session() as session:
                    from .models import Agent
                    agent = await session.get(Agent, agent_id)
                    if not agent:
                        raise HTTPException(status_code=404, detail="Agent not found")
                    agent.retired_at = None
                    session.add(agent)
                    await session.commit()

                await audit.record("agent.unretired", actor=await _actor(request), target_type="agent", target_id=str(agent_id), request=request)
                return JSONResponse({"success": True, "agent_id": agent_id, "status": "active"})
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Failed to unretire agent: {exc!s}") from exc

        # ---- Project Archive/Unarchive API ----

        @fastapi_app.post("/mail/api/archive-project", response_class=JSONResponse)
        async def archive_project_api(request: Request) -> JSONResponse:
            """Archive a project (soft-delete). Preserves all messages but hides from active lists."""
            await ensure_schema()
            try:
                body = await request.json()
                project_id: int | None = body.get("project_id")
                if project_id is None:
                    raise HTTPException(status_code=400, detail="project_id is required")

                async with get_session() as session:
                    from .models import Project
                    project = await session.get(Project, project_id)
                    if not project:
                        raise HTTPException(status_code=404, detail="Project not found")
                    project.archived_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    session.add(project)
                    await session.commit()

                await audit.record("project.archived", actor=await _actor(request), target_type="project", target_id=str(project_id), request=request)
                return JSONResponse({"success": True, "project_id": project_id, "status": "archived"})
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Failed to archive project: {exc!s}") from exc

        @fastapi_app.post("/mail/api/unarchive-project", response_class=JSONResponse)
        async def unarchive_project_api(request: Request) -> JSONResponse:
            """Restore an archived project back to active status."""
            await ensure_schema()
            try:
                body = await request.json()
                project_id: int | None = body.get("project_id")
                if project_id is None:
                    raise HTTPException(status_code=400, detail="project_id is required")

                async with get_session() as session:
                    from .models import Project
                    project = await session.get(Project, project_id)
                    if not project:
                        raise HTTPException(status_code=404, detail="Project not found")
                    project.archived_at = None
                    session.add(project)
                    await session.commit()

                await audit.record("project.unarchived", actor=await _actor(request), target_type="project", target_id=str(project_id), request=request)
                return JSONResponse({"success": True, "project_id": project_id, "status": "active"})
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Failed to unarchive project: {exc!s}") from exc

        @fastapi_app.get("/mail/projects", response_class=HTMLResponse)
        async def mail_projects_list() -> HTMLResponse:
            """Projects list view (moved from /mail)"""
            await ensure_schema()
            await refresh_project_sibling_suggestions()
            sibling_map = await get_project_sibling_data()
            async with get_session() as session:
                rows = await session.execute(
                    text("SELECT id, slug, human_key, created_at, archived_at FROM projects ORDER BY created_at DESC")
                )
                projects = []
                for r in rows.fetchall():
                    project_id = int(r[0])
                    siblings = sibling_map.get(project_id, {"confirmed": [], "suggested": []})
                    projects.append(
                        {
                            "id": project_id,
                            "slug": r[1],
                            "human_key": r[2],
                            "created_at": str(r[3]),
                            "archived_at": str(r[4]) if r[4] else None,
                            "confirmed_siblings": siblings.get("confirmed", []),
                            "suggested_siblings": siblings.get("suggested", []),
                        }
                    )
            return await _render("mail_index.html", projects=projects)

        @fastapi_app.get("/mail/{project}", response_class=HTMLResponse)
        async def mail_project(
            project: str,
            q: str | None = None,
            scope: str | None = None,
            order: str | None = None,
            boost: int | None = None,
        ) -> HTMLResponse:
            if order not in ("relevance", "time", None):
                order = "relevance"
            await ensure_schema()
            async with get_session() as session:
                proj = await session.execute(
                    text("SELECT id, slug, human_key, archived_at FROM projects WHERE slug = :k OR human_key = :k"), {"k": project}
                )
                prow = proj.fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")
                pid = int(prow[0])
                project_archived_at = str(prow[3]) if prow[3] else None
                agents_q = await session.execute(
                    text("SELECT id, name, program, model, retired_at FROM agents WHERE project_id = :pid ORDER BY name"),
                    {"pid": pid},
                )
                agents = [{"id": r[0], "name": r[1], "program": r[2], "model": r[3], "retired_at": str(r[4]) if r[4] else None} for r in agents_q.fetchall()]
                matched_messages: list[dict] = []
                if q and q.strip():
                    # Prefer FTS5 when available (fts_messages maintained by triggers)
                    fts_expr, like_pat, like_scope, tokens = _parse_fts_query(q, scope)
                    weights = (0.0, 3.0, 1.0) if (boost or 0) else (0.0, 1.0, 1.0)
                    fts_sql = (
                        "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                        "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                        "m.created_ts, m.importance, m.thread_id, "
                        "snippet(fts_messages, 2, '<mark>', '</mark>', '…', 18) AS body_snippet "
                        "FROM fts_messages "
                        "JOIN messages m ON m.id = fts_messages.rowid "
                        "JOIN agents s ON s.id = m.sender_id "
                        "LEFT JOIN projects sp ON sp.id = s.project_id "
                        "WHERE m.project_id = :pid AND fts_messages MATCH :q "
                        + (
                            "ORDER BY m.created_ts DESC "
                            if (order or "relevance") == "time"
                            else f"ORDER BY bm25(fts_messages, {weights[0]}, {weights[1]}, {weights[2]}) "
                        )
                        + "LIMIT 10000"
                    )
                    try:
                        search = await session.execute(text(fts_sql), {"pid": pid, "q": fts_expr or q})
                        matched_messages = []
                        for r in search.mappings().all():
                            sender_display, sender_meta = _http_sender_identity(
                                message_project_id=pid,
                                sender_name=r["sender_name"],
                                sender_project_id=r["sender_project_id"],
                                sender_project_human_key=r["sender_project_name"],
                                sender_project_slug=r["sender_project_slug"],
                            )
                            item = {
                                "id": r["id"],
                                "subject": r["subject"],
                                "sender": sender_display,
                                "created": str(r["created_ts"]),
                                "importance": r["importance"],
                                "thread_id": r["thread_id"],
                                "snippet": r["body_snippet"],
                                "hits": (r["body_snippet"] or "").count("<mark>"),
                            }
                            item.update(sender_meta)
                            matched_messages.append(item)
                    except Exception:
                        # Fallback to LIKE if FTS not available
                        if like_scope == "subject":
                            like_sql = (
                                "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                                "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                                "m.created_ts, m.importance, m.thread_id "
                                "FROM messages m JOIN agents s ON s.id = m.sender_id "
                                "LEFT JOIN projects sp ON sp.id = s.project_id "
                                f"WHERE m.project_id = :pid AND m.subject LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}' "
                                "ORDER BY m.created_ts DESC LIMIT 10000"
                            )
                        elif like_scope == "body":
                            like_sql = (
                                "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                                "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                                "m.created_ts, m.importance, m.thread_id "
                                "FROM messages m JOIN agents s ON s.id = m.sender_id "
                                "LEFT JOIN projects sp ON sp.id = s.project_id "
                                f"WHERE m.project_id = :pid AND m.body_md LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}' "
                                "ORDER BY m.created_ts DESC LIMIT 10000"
                            )
                        else:
                            like_sql = (
                                "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                                "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                                "m.created_ts, m.importance, m.thread_id "
                                "FROM messages m JOIN agents s ON s.id = m.sender_id "
                                "LEFT JOIN projects sp ON sp.id = s.project_id "
                                f"WHERE m.project_id = :pid AND (m.subject LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}' "
                                f"OR m.body_md LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}') "
                                "ORDER BY m.created_ts DESC LIMIT 10000"
                            )
                        search = await session.execute(text(like_sql), {"pid": pid, "pat": like_pat or f"%{_like_escape(q)}%"})
                        matched_messages = []
                        for r in search.mappings().all():
                            sender_display, sender_meta = _http_sender_identity(
                                message_project_id=pid,
                                sender_name=r["sender_name"],
                                sender_project_id=r["sender_project_id"],
                                sender_project_human_key=r["sender_project_name"],
                                sender_project_slug=r["sender_project_slug"],
                            )
                            item = {
                                "id": r["id"],
                                "subject": r["subject"],
                                "sender": sender_display,
                                "created": str(r["created_ts"]),
                                "importance": r["importance"],
                                "thread_id": r["thread_id"],
                                "snippet": "",
                                "hits": 0,
                            }
                            item.update(sender_meta)
                            matched_messages.append(item)
            return await _render(
                "mail_project.html",
                project={"id": pid, "slug": prow[1], "human_key": prow[2], "archived_at": project_archived_at},
                agents=agents,
                q=q or "",
                scope=scope or "",
                order=order or "relevance",
                boost=bool(boost),
                tokens=tokens if q and q.strip() else [],
                results=matched_messages,
            )

        @fastapi_app.get("/mail/{project}/threads", response_class=HTMLResponse)
        async def mail_threads(project: str, limit: int = 50, page: int = 1) -> HTMLResponse:
            """Browse project conversations, including standalone thread starters."""
            limit = min(max(1, limit), 200)
            page = min(max(1, page), 10000)
            await ensure_schema()
            async with get_session() as session:
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    raise HTTPException(status_code=404, detail="Project not found")

                # Group before paginating, rather than grouping a recent-message
                # slice that can hide older conversations or undercount replies.
                # Unthreaded starters use their message ID, matching mail_thread
                # and the numeric thread ID assigned by reply_message.
                rows = await session.execute(
                    text(
                        """
                        WITH project_messages AS (
                            SELECT id, subject, created_ts,
                                   CASE
                                       WHEN thread_id IS NOT NULL AND TRIM(thread_id) != ''
                                       THEN thread_id
                                       ELSE CAST(id AS TEXT)
                                   END AS thread_key
                            FROM messages
                            WHERE project_id = :pid
                        ), ranked_messages AS (
                            SELECT id, subject, created_ts, thread_key,
                                   COUNT(*) OVER (PARTITION BY thread_key) AS message_count,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY thread_key
                                       ORDER BY created_ts DESC, id DESC
                                   ) AS thread_position
                            FROM project_messages
                        )
                        SELECT thread_key, subject, created_ts, message_count
                        FROM ranked_messages
                        WHERE thread_position = 1
                        ORDER BY created_ts DESC, id DESC
                        LIMIT :lim OFFSET :off
                        """
                    ),
                    {"pid": int(prow[0]), "lim": limit + 1, "off": (page - 1) * limit},
                )
                thread_rows = rows.mappings().all()
                threads = [
                    {
                        "thread_id": row["thread_key"],
                        "subject": row["subject"],
                        "created": str(row["created_ts"]),
                        "message_count": int(row["message_count"]),
                    }
                    for row in thread_rows[:limit]
                ]

            return await _render(
                "mail_threads.html",
                project={"slug": prow[1], "human_key": prow[2]},
                threads=threads,
                page=page,
                limit=limit,
                prev_page=page - 1 if page > 1 else None,
                next_page=page + 1 if len(thread_rows) > limit and page < 10000 else None,
            )

        @fastapi_app.post("/mail/api/projects/{project_id}/siblings/{other_id}", response_class=JSONResponse)
        async def update_project_sibling(project_id: int, other_id: int, request: Request) -> JSONResponse:
            try:
                payload = await request.json()
            except Exception:
                payload = {}
            action = str(payload.get("action", "")).lower()
            if action not in {"confirm", "dismiss", "reset"}:
                return JSONResponse({"error": "Invalid action"}, status_code=status.HTTP_400_BAD_REQUEST)

            target_status = {
                "confirm": "confirmed",
                "dismiss": "dismissed",
                "reset": "suggested",
            }[action]

            try:
                suggestion = await update_project_sibling_status(project_id, other_id, target_status)
            except ValueError as exc:
                return JSONResponse({"error": str(exc)}, status_code=status.HTTP_400_BAD_REQUEST)
            except NoResultFound:
                return JSONResponse({"error": "Project pair not found"}, status_code=status.HTTP_404_NOT_FOUND)
            except Exception as exc:
                structlog.get_logger("sibling").exception(
                    "project_sibling.update_failed",
                    project_id=project_id,
                    other_id=other_id,
                    action=action,
                    error=str(exc),
                )
                return JSONResponse(
                    {"error": "Unable to update sibling status"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            return JSONResponse({"status": suggestion["status"], "suggestion": suggestion})

        @fastapi_app.get("/mail/unified-inbox", response_class=HTMLResponse)
        async def unified_inbox(limit: int = 10000, filter_importance: str | None = None) -> HTMLResponse:
            """Unified inbox showing messages from all active agents across all projects."""
            limit = min(max(1, limit), 10000)
            await ensure_schema()
            async with get_session() as session:
                # Get all projects with their agents
                projects_query = await session.execute(
                    text(
                        """
                    SELECT p.id, p.slug, p.human_key,
                           COUNT(DISTINCT a.id) as agent_count,
                           MAX(a.last_active_ts) as last_activity
                    FROM projects p
                    LEFT JOIN agents a ON a.project_id = p.id
                    GROUP BY p.id, p.slug, p.human_key
                    ORDER BY (last_activity IS NULL) ASC, last_activity DESC, p.created_at DESC
                    """
                    )
                )
                projects_data = []
                for r in projects_query.fetchall():
                    proj_id = int(r[0])
                    # Get agents for this project
                    agents_query = await session.execute(
                        text(
                            """
                        SELECT a.id, a.name, a.program, a.model, a.last_active_ts
                        FROM agents a
                        WHERE a.project_id = :pid
                        ORDER BY a.last_active_ts DESC, a.name ASC
                        """
                        ),
                        {"pid": proj_id},
                    )

                    agents_list = []
                    for ar in agents_query.fetchall():
                        agents_list.append(
                            {
                                "id": int(ar[0]),
                                "name": ar[1],
                                "program": ar[2],
                                "model": ar[3],
                                "last_active": str(ar[4]) if ar[4] else None,
                            }
                        )

                    if agents_list:  # Only include projects with agents
                        projects_data.append(
                            {
                                "id": proj_id,
                                "slug": r[1],
                                "human_key": r[2],
                                "agent_count": int(r[3] or 0),
                                "agents": agents_list,
                            }
                        )

                # Get recent messages across all projects with thread information
                # Build WHERE clause safely using parameterized queries
                importance_conditions = []
                query_params = {"lim": limit}

                if filter_importance and filter_importance.lower() in ["urgent", "high"]:
                    importance_conditions.append("m.importance IN ('urgent', 'high')")

                where_clause = "WHERE " + " AND ".join(importance_conditions) if importance_conditions else "WHERE 1=1"

                messages_query = await session.execute(
                    text(
                        f"""
                    SELECT
                        m.id, m.subject, m.body_md, m.created_ts, m.importance, m.thread_id,
                        m.project_id AS message_project_id,
                        p.slug, p.human_key,
                        sender.name as sender_name,
                        sender.project_id AS sender_project_id,
                        sp.human_key AS sender_project_name,
                        sp.slug AS sender_project_slug,
                        COALESCE(
                            (
                                SELECT GROUP_CONCAT(name, ', ')
                                FROM (
                                    SELECT DISTINCT recip2.name AS name
                                    FROM message_recipients mr2
                                    JOIN agents recip2 ON recip2.id = mr2.agent_id
                                    WHERE mr2.message_id = m.id
                                    ORDER BY name
                                )
                            ),
                            ''
                        ) as recipient_names,
                        COUNT(DISTINCT CASE WHEN m2.id IS NOT NULL THEN m2.id END) as thread_count
                    FROM messages m
                    JOIN projects p ON p.id = m.project_id
                    JOIN agents sender ON sender.id = m.sender_id
                    LEFT JOIN projects sp ON sp.id = sender.project_id
                    LEFT JOIN message_recipients mr ON mr.message_id = m.id
                    LEFT JOIN agents recip ON recip.id = mr.agent_id
                    LEFT JOIN messages m2 ON (
                        m.thread_id IS NOT NULL
                        AND m2.thread_id = m.thread_id
                        AND m2.project_id = m.project_id
                        AND m2.id != m.id
                    )
                    {where_clause}
                    GROUP BY m.id, m.subject, m.body_md, m.created_ts, m.importance, m.thread_id,
                             m.project_id, p.slug, p.human_key, sender.name, sender.project_id, sp.human_key, sp.slug
                    ORDER BY m.created_ts DESC
                    LIMIT :lim
                    """
                    ),
                    query_params,
                )

                messages = []
                for r in messages_query.mappings().all():
                    sender_display, sender_meta = _http_sender_identity(
                        message_project_id=r["message_project_id"],
                        sender_name=r["sender_name"],
                        sender_project_id=r["sender_project_id"],
                        sender_project_human_key=r["sender_project_name"],
                        sender_project_slug=r["sender_project_slug"],
                    )
                    item = {
                        "id": int(r["id"]),
                        "subject": r["subject"],
                        "body_md": r["body_md"] or "",
                        "created": str(r["created_ts"]),
                        "importance": r["importance"] or "normal",
                        "thread_id": r["thread_id"],
                        "project_slug": r["slug"],
                        "project_name": r["human_key"],
                        "sender": sender_display,
                        "recipients": r["recipient_names"] or "",
                        "thread_count": int(r["thread_count"] or 0),
                    }
                    item.update(sender_meta)
                    messages.append(item)

            return await _render(
                "mail_unified_inbox.html",
                projects=projects_data,
                messages=messages,
                total_agents=sum(p["agent_count"] for p in projects_data),
                total_messages=len(messages),
                filter_importance=filter_importance or "",
            )

        @fastapi_app.get("/mail/{project}/inbox/{agent}", response_class=HTMLResponse)
        async def mail_inbox(project: str, agent: str, limit: int = 10000, page: int = 1) -> HTMLResponse:
            limit = min(max(1, limit), 10000)
            page = min(max(1, page), 10000)
            await ensure_schema()
            async with get_session() as session:
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")
                pid = int(prow[0])
                arow = (
                    await session.execute(
                        text("SELECT id, name FROM agents WHERE project_id = :pid AND lower(name) = lower(:name)"),
                        {"pid": pid, "name": agent},
                    )
                ).fetchone()
                if not arow:
                    return await _render("error.html", message="Agent not found")
                offset = max(0, (max(1, page) - 1) * max(1, limit))
                inbox_rows = await session.execute(
                    text(
                        """
                    SELECT
                        m.id,
                        m.subject,
                        s.name AS sender_name,
                        s.project_id AS sender_project_id,
                        sp.human_key AS sender_project_name,
                        sp.slug AS sender_project_slug,
                        m.created_ts,
                        m.importance,
                        m.thread_id,
                        m.ack_required,
                        mr.read_ts,
                        mr.ack_ts
                    FROM messages m
                    JOIN message_recipients mr ON mr.message_id = m.id
                    JOIN agents a ON a.id = mr.agent_id
                    JOIN agents s ON s.id = m.sender_id
                    LEFT JOIN projects sp ON sp.id = s.project_id
                    WHERE m.project_id = :pid AND a.name = :name
                    ORDER BY m.created_ts DESC
                    LIMIT :lim OFFSET :off
                    """
                    ),
                    {"pid": pid, "name": agent, "lim": limit, "off": offset},
                )
                items = []
                for r in inbox_rows.mappings().all():
                    sender_display, sender_meta = _http_sender_identity(
                        message_project_id=pid,
                        sender_name=r["sender_name"],
                        sender_project_id=r["sender_project_id"],
                        sender_project_human_key=r["sender_project_name"],
                        sender_project_slug=r["sender_project_slug"],
                    )
                    read_ts = r["read_ts"]
                    ack_ts = r["ack_ts"]
                    ack_required = bool(r["ack_required"])
                    item = {
                        "id": r["id"],
                        "subject": r["subject"],
                        "sender": sender_display,
                        "created": str(r["created_ts"]),
                        "importance": r["importance"],
                        "thread_id": r["thread_id"],
                        "ack_required": ack_required,
                        "read_ts": str(read_ts) if read_ts else None,
                        "ack_ts": str(ack_ts) if ack_ts else None,
                        "unread": read_ts is None,
                        "needs_ack": ack_required and ack_ts is None,
                        "acked": ack_ts is not None,
                    }
                    item.update(sender_meta)
                    items.append(item)
            return await _render(
                "mail_inbox.html",
                project={"slug": prow[1], "human_key": prow[2]},
                agent=agent,
                items=items,
                page=page,
                limit=limit,
                next_page=page + 1,
                prev_page=page - 1 if page > 1 else None,
            )

        @fastapi_app.get("/mail/{project}/message/{mid}", response_class=HTMLResponse)
        async def mail_message(project: str, mid: int) -> HTMLResponse:
            await ensure_schema()
            async with get_session() as session:
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")
                pid = int(prow[0])
                mrow = (
                    await session.execute(
                        text(
                            """
                            SELECT
                                m.id,
                                m.subject,
                                m.body_md,
                                s.name AS sender_name,
                                s.project_id AS sender_project_id,
                                sp.human_key AS sender_project_name,
                                sp.slug AS sender_project_slug,
                                m.created_ts,
                                m.importance,
                                m.thread_id,
                                m.ack_required,
                                m.attachments
                            FROM messages m
                            JOIN agents s ON s.id = m.sender_id
                            LEFT JOIN projects sp ON sp.id = s.project_id
                            WHERE m.project_id = :pid AND m.id = :mid
                            """
                        ),
                        {"pid": pid, "mid": mid},
                    )
                ).mappings().fetchone()
                if not mrow:
                    return await _render("error.html", message="Message not found")
                recs = await session.execute(
                    text(
                        "SELECT a.name, mr.kind, mr.read_ts, mr.ack_ts "
                        "FROM message_recipients mr JOIN agents a ON a.id = mr.agent_id "
                        "WHERE mr.message_id = :mid"
                    ),
                    {"mid": mid},
                )
                recipients = [
                    {
                        "name": r[0],
                        "kind": r[1],
                        "read_ts": str(r[2]) if r[2] else None,
                        "ack_ts": str(r[3]) if r[3] else None,
                    }
                    for r in recs.fetchall()
                ]
                ack_required_msg = bool(mrow["ack_required"])
                ack_count = sum(1 for r in recipients if r["ack_ts"])
                read_count = sum(1 for r in recipients if r["read_ts"])
                ack_summary = {
                    "ack_required": ack_required_msg,
                    "total": len(recipients),
                    "read": read_count,
                    "acked": ack_count,
                }
                # Find thread messages if thread_id is set
                thread_items: list[dict] = []
                th = mrow["thread_id"]
                if isinstance(th, str) and th.strip():
                    th_rows = await session.execute(
                        text(
                            """
                            SELECT
                                m.id,
                                m.subject,
                                s.name AS sender_name,
                                s.project_id AS sender_project_id,
                                sp.human_key AS sender_project_name,
                                sp.slug AS sender_project_slug,
                                m.created_ts
                            FROM messages m
                            JOIN agents s ON s.id = m.sender_id
                            LEFT JOIN projects sp ON sp.id = s.project_id
                            WHERE m.project_id = :pid AND (m.thread_id = :th OR m.id = :id)
                            ORDER BY m.created_ts ASC
                            """
                        ),
                        {"pid": pid, "th": th, "id": mid},
                    )
                    thread_items = []
                    for rr in th_rows.mappings().all():
                        sender_display, sender_meta = _http_sender_identity(
                            message_project_id=pid,
                            sender_name=rr["sender_name"],
                            sender_project_id=rr["sender_project_id"],
                            sender_project_human_key=rr["sender_project_name"],
                            sender_project_slug=rr["sender_project_slug"],
                        )
                        item = {
                            "id": rr["id"],
                            "subject": rr["subject"],
                            "from": sender_display,
                            "created": str(rr["created_ts"]),
                        }
                        item.update(sender_meta)
                        thread_items.append(item)
            # Convert markdown body to HTML for display (server-side render)
            body_html = (
                markdown2.markdown(mrow["body_md"] or "", extras=["fenced-code-blocks", "tables", "strike", "cuddled-lists"])
                if mrow["body_md"]
                else ""
            )
            if body_html:
                body_html = _html_cleaner.clean(body_html)

            # Get commit SHA for provenance badge
            commit_sha = None
            try:
                settings = get_settings()
                archive = await ensure_archive(settings, prow[1])
                commit_sha = await get_message_commit_sha(archive, mid)
            except Exception:
                pass  # Commit SHA is optional

            sender_display, sender_meta = _http_sender_identity(
                message_project_id=pid,
                sender_name=mrow["sender_name"],
                sender_project_id=mrow["sender_project_id"],
                sender_project_human_key=mrow["sender_project_name"],
                sender_project_slug=mrow["sender_project_slug"],
            )
            # Parse persisted attachments so the message view can render/link
            # them (#220). Stored as a JSON array column.
            message_attachments: list[dict[str, Any]] = []
            try:
                raw_attachments = mrow["attachments"]
                if isinstance(raw_attachments, str):
                    try:
                        parsed_attachments = json.loads(raw_attachments)
                    except json.JSONDecodeError:
                        parsed_attachments = []
                else:
                    parsed_attachments = raw_attachments
                if isinstance(parsed_attachments, list):
                    message_attachments = [a for a in parsed_attachments if isinstance(a, dict)]
            except Exception:
                message_attachments = []

            message_payload = {
                "id": mrow["id"],
                "subject": mrow["subject"],
                "body_md": mrow["body_md"],
                "body_html": body_html,
                "sender": sender_display,
                "created": str(mrow["created_ts"]),
                "importance": mrow["importance"],
                "thread_id": mrow["thread_id"],
                "attachments": message_attachments,
            }
            message_payload.update(sender_meta)

            return await _render(
                "mail_message.html",
                project={"slug": prow[1], "human_key": prow[2]},
                message=message_payload,
                recipients=recipients,
                ack_summary=ack_summary,
                thread_items=thread_items,
                commit_sha=commit_sha,
            )

        @fastapi_app.post("/mail/{project}/inbox/{agent}/mark-read")
        async def mark_selected_messages_read(project: str, agent: str, request: Request) -> JSONResponse:
            """Mark specific messages as read for an agent."""
            await ensure_schema()

            try:
                # Parse request body
                request_body = await request.json()
                message_ids: list[int] = request_body.get("message_ids", [])

                if not message_ids:
                    raise HTTPException(status_code=400, detail="No message IDs provided")

                # Limit to prevent SQL parameter overflow (SQLite default limit is 999)
                # Also prevents abuse - if someone wants to mark 1000+ messages, use "mark all"
                if len(message_ids) > 500:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Too many messages selected ({len(message_ids)}). Maximum is 500. Use 'Mark All Read' instead."
                    )

                async with get_session() as session:
                    # Get project
                    prow = (
                        await session.execute(
                            text("SELECT id, slug FROM projects WHERE slug = :k OR human_key = :k"),
                            {"k": project},
                        )
                    ).fetchone()
                    if not prow:
                        raise HTTPException(status_code=404, detail="Project not found")

                    pid = int(prow[0])

                    # Get agent
                    arow = (
                        await session.execute(
                            text("SELECT id FROM agents WHERE project_id = :pid AND name = :name"),
                            {"pid": pid, "name": agent},
                        )
                    ).fetchone()
                    if not arow:
                        raise HTTPException(status_code=404, detail="Agent not found")

                    aid = int(arow[0])

                    # Mark specific messages as read
                    # Use naive UTC datetime for SQLite compatibility
                    now = datetime.now(timezone.utc).replace(tzinfo=None)

                    # Use IN clause with parameter binding
                    placeholders = ','.join([f':mid{i}' for i in range(len(message_ids))])
                    params = {"aid": aid, "now": now}
                    params.update({f"mid{i}": mid for i, mid in enumerate(message_ids)})

                    result = await session.execute(
                        text(
                            f"""
                            UPDATE message_recipients
                            SET read_ts = :now
                            WHERE agent_id = :aid
                            AND message_id IN ({placeholders})
                            AND read_ts IS NULL
                            RETURNING message_id
                            """
                        ),
                        params,
                    )
                    marked_ids = [int(row[0]) for row in result.fetchall()]
                    await _add_read_events(session, agent, marked_ids)
                    await session.commit()

                    count = len(marked_ids)

                    return JSONResponse({
                        "success": True,
                        "marked_count": count,
                        "requested_count": len(message_ids),
                        "agent": agent,
                        "project": prow[1],
                    })

            except HTTPException:
                raise
            except Exception as exc:
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"Failed to mark messages as read: {exc!s}") from exc

        @fastapi_app.post("/mail/{project}/inbox/{agent}/mark-all-read")
        async def mark_all_messages_read(project: str, agent: str) -> JSONResponse:
            """Mark all messages for an agent as read."""
            await ensure_schema()

            try:
                async with get_session() as session:
                    # Get project
                    prow = (
                        await session.execute(
                            text("SELECT id, slug FROM projects WHERE slug = :k OR human_key = :k"),
                            {"k": project},
                        )
                    ).fetchone()
                    if not prow:
                        raise HTTPException(status_code=404, detail="Project not found")

                    pid = int(prow[0])

                    # Get agent
                    arow = (
                        await session.execute(
                            text("SELECT id FROM agents WHERE project_id = :pid AND name = :name"),
                            {"pid": pid, "name": agent},
                        )
                    ).fetchone()
                    if not arow:
                        raise HTTPException(status_code=404, detail="Agent not found")

                    aid = int(arow[0])

                    # Mark all unread messages as read
                    # Use naive UTC datetime for SQLite compatibility
                    now = datetime.now(timezone.utc).replace(tzinfo=None)
                    result = await session.execute(
                        text(
                            """
                            UPDATE message_recipients
                            SET read_ts = :now
                            WHERE agent_id = :aid
                            AND read_ts IS NULL
                            RETURNING message_id
                            """
                        ),
                        {"aid": aid, "now": now},
                    )
                    marked_ids = [int(row[0]) for row in result.fetchall()]
                    await _add_read_events(session, agent, marked_ids)
                    await session.commit()

                    count = len(marked_ids)

                    return JSONResponse({
                        "success": True,
                        "marked_count": count,
                        "agent": agent,
                        "project": prow[1],
                    })

            except HTTPException:
                raise
            except Exception as exc:
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"Failed to mark messages as read: {exc!s}") from exc

        @fastapi_app.post("/mail/{project}/inbox/{agent}/delete-messages")
        async def delete_selected_messages(project: str, agent: str, request: Request) -> JSONResponse:
            """Permanently delete specific messages for an agent.

            Removes messages from the SQLite database AND deletes the
            corresponding markdown files from the Git archive so that
            messages do not reappear after a refresh or server restart.
            """
            await ensure_schema()

            try:
                request_body = await request.json()
                message_ids: list[int] = request_body.get("message_ids", [])

                if not message_ids:
                    raise HTTPException(status_code=400, detail="No message IDs provided")

                if len(message_ids) > 500:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Too many messages selected ({len(message_ids)}). Maximum is 500."
                    )

                deleted_count = 0
                messages_to_delete: list[tuple[Any, ...]] = []
                recip_map: dict[int, list[str]] = {}
                async with get_session() as session:
                    # Resolve project
                    prow = (
                        await session.execute(
                            text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                            {"k": project},
                        )
                    ).fetchone()
                    if not prow:
                        raise HTTPException(status_code=404, detail="Project not found")

                    pid = int(prow[0])
                    project_slug = prow[1]

                    # Resolve agent
                    arow = (
                        await session.execute(
                            text("SELECT id FROM agents WHERE project_id = :pid AND name = :name"),
                            {"pid": pid, "name": agent},
                        )
                    ).fetchone()
                    if not arow:
                        raise HTTPException(status_code=404, detail="Agent not found")

                    # Fetch message metadata before deleting so we can locate Git files
                    placeholders = ','.join([f':mid{i}' for i in range(len(message_ids))])
                    id_params: dict[str, Any] = {"pid": pid}
                    id_params.update({f"mid{i}": mid for i, mid in enumerate(message_ids)})

                    rows = await session.execute(
                        text(
                            f"""
                            SELECT m.id, m.created_ts, m.subject, s.name AS sender_name
                            FROM messages m
                            JOIN agents s ON s.id = m.sender_id
                            WHERE m.project_id = :pid
                            AND m.id IN ({placeholders})
                            """
                        ),
                        id_params,
                    )
                    messages_to_delete = [tuple(row) for row in rows.fetchall()]

                    if not messages_to_delete:
                        return JSONResponse({"success": True, "deleted_count": 0})

                    # Collect recipient names per message for inbox path removal
                    recip_rows = await session.execute(
                        text(
                            f"""
                            SELECT mr.message_id, a.name
                            FROM message_recipients mr
                            JOIN agents a ON a.id = mr.agent_id
                            WHERE mr.message_id IN ({placeholders})
                            """
                        ),
                        {f"mid{i}": mid for i, mid in enumerate(message_ids)},
                    )
                    for rr in recip_rows.fetchall():
                        recip_map.setdefault(int(rr[0]), []).append(rr[1])

                    # Delete from SQLite: recipients first, then messages
                    await session.execute(
                        text(
                            f"DELETE FROM message_recipients WHERE message_id IN ({placeholders})"
                        ),
                        {f"mid{i}": mid for i, mid in enumerate(message_ids)},
                    )
                    del_result = await session.execute(
                        text(
                            f"DELETE FROM messages WHERE project_id = :pid AND id IN ({placeholders})"
                        ),
                        id_params,
                    )
                    deleted_count = int(getattr(del_result, "rowcount", 0) or 0)
                    await session.commit()

                settings = get_settings()
                git_files_removed = 0
                try:
                    git_files_removed = await _delete_messages_from_archive(
                        settings=settings,
                        project_slug=project_slug,
                        messages_to_delete=messages_to_delete,
                        recip_map=recip_map,
                        commit_message=f"delete: {deleted_count} message(s) via web UI\n",
                    )
                except Exception as archive_exc:
                    # Archive operations are best-effort; DB deletion already happened.
                    logging.getLogger(__name__).warning(
                        "Git archive cleanup failed: %s", archive_exc
                    )

                await audit.record(
                    "messages.deleted", actor=await _actor(request), target_type="inbox",
                    target_id=f"{project_slug}/{agent}", details=f"deleted={deleted_count}", request=request,
                )
                return JSONResponse({
                    "success": True,
                    "deleted_count": deleted_count,
                    "git_files_removed": git_files_removed,
                    "agent": agent,
                    "project": project_slug,
                })

            except HTTPException:
                raise
            except Exception as exc:
                import traceback
                traceback.print_exc()
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to delete messages: {exc!s}"
                ) from exc

        @fastapi_app.get("/mail/{project}/thread/{thread_id}", response_class=HTMLResponse)
        async def mail_thread(project: str, thread_id: str) -> HTMLResponse:
            """Display all messages in a thread chronologically (Gmail-style conversation view).

            NOTE: Currently loads ALL messages in thread without pagination.
            For threads with 1000+ messages, consider adding LIMIT/OFFSET pagination.
            """
            await ensure_schema()
            async with get_session() as session:
                # Get project
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")

                pid = int(prow[0])

                # Get all messages in this thread, ordered chronologically
                # Include messages where thread_id matches OR message id matches (for thread starter)
                try:
                    thread_id_int = int(thread_id)
                    rows = await session.execute(
                        text(
                            """
                            SELECT
                                m.id,
                                m.subject,
                                m.body_md,
                                s.name AS sender_name,
                                s.project_id AS sender_project_id,
                                sp.human_key AS sender_project_name,
                                sp.slug AS sender_project_slug,
                                m.created_ts,
                                m.importance,
                                m.thread_id
                            FROM messages m
                            JOIN agents s ON s.id = m.sender_id
                            LEFT JOIN projects sp ON sp.id = s.project_id
                            WHERE m.project_id = :pid
                            AND (m.thread_id = :tid OR m.id = :tid_int)
                            ORDER BY m.created_ts ASC
                            """
                        ),
                        {"pid": pid, "tid": thread_id, "tid_int": thread_id_int},
                    )
                except ValueError:
                    # Not an integer, just use string thread_id
                    rows = await session.execute(
                        text(
                            """
                            SELECT
                                m.id,
                                m.subject,
                                m.body_md,
                                s.name AS sender_name,
                                s.project_id AS sender_project_id,
                                sp.human_key AS sender_project_name,
                                sp.slug AS sender_project_slug,
                                m.created_ts,
                                m.importance,
                                m.thread_id
                            FROM messages m
                            JOIN agents s ON s.id = m.sender_id
                            LEFT JOIN projects sp ON sp.id = s.project_id
                            WHERE m.project_id = :pid
                            AND m.thread_id = :tid
                            ORDER BY m.created_ts ASC
                            """
                        ),
                        {"pid": pid, "tid": thread_id},
                    )

                messages = []
                for r in rows.mappings().all():
                    # Convert markdown to HTML for each message
                    body_html = ""
                    if r["body_md"]:
                        body_html = markdown2.markdown(
                            r["body_md"],
                            extras=["fenced-code-blocks", "tables", "strike", "cuddled-lists"]
                        )
                        body_html = _html_cleaner.clean(body_html)

                    sender_display, sender_meta = _http_sender_identity(
                        message_project_id=pid,
                        sender_name=r["sender_name"],
                        sender_project_id=r["sender_project_id"],
                        sender_project_human_key=r["sender_project_name"],
                        sender_project_slug=r["sender_project_slug"],
                    )
                    message = {
                        "id": r["id"],
                        "subject": r["subject"],
                        "body_md": r["body_md"],
                        "body_html": body_html,
                        "sender": sender_display,
                        "created": str(r["created_ts"]),
                        "importance": r["importance"],
                        "thread_id": r["thread_id"],
                    }
                    message.update(sender_meta)
                    messages.append(message)

                if not messages:
                    return await _render(
                        "error.html",
                        message=f"No messages found in thread '{thread_id}'. The thread may not exist or all messages may have been deleted."
                    )

                # Get unique subject (use first message's subject, with fallback)
                thread_subject = messages[0]["subject"] if messages and messages[0]["subject"] else f"Thread {thread_id}"

                return await _render(
                    "mail_thread.html",
                    project={"slug": prow[1], "human_key": prow[2]},
                    thread_id=thread_id,
                    thread_subject=thread_subject,
                    messages=messages,
                    message_count=len(messages),
                )

        # Full-text search UI across subject/body using LIKE fallback (SQLite FTS handled elsewhere)
        @fastapi_app.get("/mail/{project}/search", response_class=HTMLResponse)
        async def mail_search(
            project: str,
            q: str,
            limit: int = 10000,
            scope: str | None = None,
            order: str | None = None,
            boost: int | None = None,
        ) -> HTMLResponse:
            limit = min(max(1, limit), 10000)
            if order not in ("relevance", "time", None):
                order = "relevance"
            await ensure_schema()
            async with get_session() as session:
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")
                pid = int(prow[0])
                fts_expr, like_pat, like_scope, tokens = _parse_fts_query(q, scope)
                weights = (0.0, 3.0, 1.0) if (boost or 0) else (0.0, 1.0, 1.0)
                fts_sql = (
                    "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                    "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                    "m.created_ts, m.importance, m.thread_id, "
                    "snippet(fts_messages, 2, '<mark>', '</mark>', '…', 22) AS body_snippet "
                    "FROM fts_messages "
                    "JOIN messages m ON m.id = fts_messages.rowid "
                    "JOIN agents s ON s.id = m.sender_id "
                    "LEFT JOIN projects sp ON sp.id = s.project_id "
                    "WHERE m.project_id = :pid AND fts_messages MATCH :q "
                    + (
                        "ORDER BY m.created_ts DESC "
                        if (order or "relevance") == "time"
                        else f"ORDER BY bm25(fts_messages, {weights[0]}, {weights[1]}, {weights[2]}) "
                    )
                    + "LIMIT :lim"
                )
                try:
                    rows = await session.execute(text(fts_sql), {"pid": pid, "q": fts_expr or q, "lim": limit})
                    results = []
                    for r in rows.mappings().all():
                        sender_display, sender_meta = _http_sender_identity(
                            message_project_id=pid,
                            sender_name=r["sender_name"],
                            sender_project_id=r["sender_project_id"],
                            sender_project_human_key=r["sender_project_name"],
                            sender_project_slug=r["sender_project_slug"],
                        )
                        item = {
                            "id": r["id"],
                            "subject": r["subject"],
                            "from": sender_display,
                            "created": str(r["created_ts"]),
                            "importance": r["importance"],
                            "thread_id": r["thread_id"],
                            "snippet": r["body_snippet"],
                            "hits": (r["body_snippet"] or "").count("<mark>"),
                        }
                        item.update(sender_meta)
                        results.append(item)
                except Exception:
                    if like_scope == "subject":
                        like_sql = (
                            "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                            "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                            "m.created_ts, m.importance, m.thread_id "
                            "FROM messages m JOIN agents s ON s.id = m.sender_id "
                            "LEFT JOIN projects sp ON sp.id = s.project_id "
                            f"WHERE m.project_id = :pid AND m.subject LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}' "
                            "ORDER BY m.created_ts DESC LIMIT :lim"
                        )
                    elif like_scope == "body":
                        like_sql = (
                            "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                            "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                            "m.created_ts, m.importance, m.thread_id "
                            "FROM messages m JOIN agents s ON s.id = m.sender_id "
                            "LEFT JOIN projects sp ON sp.id = s.project_id "
                            f"WHERE m.project_id = :pid AND m.body_md LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}' "
                            "ORDER BY m.created_ts DESC LIMIT :lim"
                        )
                    else:
                        like_sql = (
                            "SELECT m.id, m.subject, s.name AS sender_name, s.project_id AS sender_project_id, "
                            "sp.human_key AS sender_project_name, sp.slug AS sender_project_slug, "
                            "m.created_ts, m.importance, m.thread_id "
                            "FROM messages m JOIN agents s ON s.id = m.sender_id "
                            "LEFT JOIN projects sp ON sp.id = s.project_id "
                            f"WHERE m.project_id = :pid AND (m.subject LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}' "
                            f"OR m.body_md LIKE :pat ESCAPE '{_LIKE_ESCAPE_CHAR}') "
                            "ORDER BY m.created_ts DESC LIMIT :lim"
                        )
                    rows = await session.execute(
                        text(like_sql), {"pid": pid, "pat": like_pat or f"%{_like_escape(q)}%", "lim": limit}
                    )
                    results = []
                    for r in rows.mappings().all():
                        sender_display, sender_meta = _http_sender_identity(
                            message_project_id=pid,
                            sender_name=r["sender_name"],
                            sender_project_id=r["sender_project_id"],
                            sender_project_human_key=r["sender_project_name"],
                            sender_project_slug=r["sender_project_slug"],
                        )
                        item = {
                            "id": r["id"],
                            "subject": r["subject"],
                            "from": sender_display,
                            "created": str(r["created_ts"]),
                            "importance": r["importance"],
                            "thread_id": r["thread_id"],
                            "snippet": "",
                            "hits": 0,
                        }
                        item.update(sender_meta)
                        results.append(item)
            return await _render(
                "mail_search.html",
                project={"slug": prow[1], "human_key": prow[2]},
                q=q,
                scope=scope or "",
                order=order or "relevance",
                tokens=tokens,
                results=results,
                boost=bool(boost),
            )

        # File reservations and attachments views
        @fastapi_app.get("/mail/{project}/file_reservations", response_class=HTMLResponse)
        async def mail_file_reservations(project: str) -> HTMLResponse:
            await ensure_schema()
            async with get_session() as session:
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")
                pid = int(prow[0])
                # LEFT JOIN so orphaned reservations whose owning agent row
                # has been deleted still surface in the web UI (`a.name` will
                # be NULL — render as "<orphaned>" so operators see them and
                # can act). Matches the model-side LEFT JOIN in
                # _collect_file_reservation_statuses. (#161)
                rows = await session.execute(
                    text(
                        "SELECT c.id, a.name, c.path_pattern, c.exclusive, c.created_ts, c.expires_ts, c.released_ts, c.agent_id FROM file_reservations c LEFT JOIN agents a ON a.id = c.agent_id WHERE c.project_id = :pid ORDER BY c.created_ts DESC"
                    ),
                    {"pid": pid},
                )
                file_reservations = [
                    {
                        "id": r[0],
                        "agent": r[1] if r[1] is not None else "<orphaned>",
                        "agent_id": r[7],
                        "path_pattern": r[2],
                        "exclusive": bool(r[3]),
                        "created": str(r[4]),
                        "expires": str(r[5]) if r[5] else "",
                        "released": str(r[6]) if r[6] else "",
                    }
                    for r in rows.fetchall()
                ]
            return await _render("mail_file_reservations.html", project={"slug": prow[1], "human_key": prow[2]}, file_reservations=file_reservations)

        @fastapi_app.get("/mail/{project}/attachments", response_class=HTMLResponse)
        async def mail_attachments(project: str) -> HTMLResponse:
            await ensure_schema()
            async with get_session() as session:
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")
                pid = int(prow[0])
                rows = await session.execute(
                    text(
                        "SELECT id, subject, created_ts, attachments FROM messages WHERE project_id = :pid AND json_array_length(attachments) > 0 ORDER BY created_ts DESC LIMIT 10000"
                    ),
                    {"pid": pid},
                )
                items = []
                for r in rows.fetchall():
                    attachments: list[dict[str, Any]] = []
                    try:
                        raw = r[3]
                        if isinstance(raw, str):
                            try:
                                parsed = json.loads(raw)
                            except json.JSONDecodeError:
                                parsed = []
                        else:
                            parsed = raw
                        if isinstance(parsed, list):
                            attachments = [a for a in parsed if isinstance(a, dict)]
                    except Exception:
                        attachments = []
                    items.append({"id": r[0], "subject": r[1], "created": str(r[2]), "attachments": attachments})
            return await _render("mail_attachments.html", project={"slug": prow[1], "human_key": prow[2]}, items=items)

        # ========== Human Overseer Routes ==========

        @fastapi_app.get("/mail/{project}/overseer/compose", response_class=HTMLResponse)
        async def overseer_compose(project: str) -> HTMLResponse:
            """Display Human Overseer message composer."""
            await ensure_schema()
            async with get_session() as session:
                # Get project
                prow = (
                    await session.execute(
                        text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                        {"k": project},
                    )
                ).fetchone()
                if not prow:
                    return await _render("error.html", message="Project not found")

                # Get all agents for this project
                pid = int(prow[0])
                agent_rows = await session.execute(
                    text("SELECT name FROM agents WHERE project_id = :pid ORDER BY name"),
                    {"pid": pid}
                )
                agents = [{"name": r[0]} for r in agent_rows.fetchall()]

            return await _render(
                "overseer_compose.html",
                project={"slug": prow[1], "human_key": prow[2]},
                agents=agents
            )

        @fastapi_app.post("/mail/{project}/overseer/send")
        async def overseer_send(project: str, request: Request) -> JSONResponse:
            """Send message from Human Overseer to selected agents."""
            await ensure_schema()

            try:
                # Parse request body
                request_body = await request.json()
                recipients: list[str] = request_body.get("recipients", [])
                subject: str = request_body.get("subject", "").strip()
                body_md: str = request_body.get("body_md", "").strip()
                thread_id: str | None = request_body.get("thread_id")

                # Comprehensive validation
                if not recipients:
                    raise HTTPException(status_code=400, detail="At least one recipient is required")
                if len(recipients) > 100:
                    raise HTTPException(status_code=400, detail="Too many recipients (maximum 100 agents)")
                if not subject:
                    raise HTTPException(status_code=400, detail="Subject is required")
                if len(subject) > 200:
                    raise HTTPException(status_code=400, detail="Subject too long (maximum 200 characters)")
                if not body_md:
                    raise HTTPException(status_code=400, detail="Message body is required")
                if len(body_md) > 50000:
                    raise HTTPException(status_code=400, detail="Message body too long (maximum 50,000 characters)")

                # Remove duplicate recipients while preserving order
                recipients = list(dict.fromkeys(recipients))

                # Add Human Overseer preamble (pure markdown for cross-renderer compatibility)
                preamble = """---

        🚨 MESSAGE FROM HUMAN OVERSEER 🚨

        This message is from a human operator overseeing this project. Please prioritize the instructions below over your current tasks.

        You should:
        1. Temporarily pause your current work
        2. Complete the request described below
        3. Resume your original plans afterward (unless modified by these instructions)

        The human's guidance supersedes all other priorities.

        ---

        """
                full_body = preamble + body_md

                # Validate combined length (preamble + user message)
                if len(full_body) > 50000:
                    preamble_length = len(preamble)
                    max_user_length = 50000 - preamble_length
                    raise HTTPException(
                        status_code=400,
                        detail=f"Message body too long ({len(body_md)} characters). Maximum is {max_user_length} characters to accommodate the overseer preamble ({preamble_length} characters)."
                    )

                # Keep database work and archive work in separate phases so
                # the request never holds a live DB transaction while doing
                # archive/Git I/O.
                from datetime import datetime, timezone
                message_id: int | None = None
                valid_recipients: list[str] = []
                project_slug = ""
                project_human_key = ""
                overseer_name = "HumanOverseer"
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                async with get_session() as session:
                    # Get project
                    prow = (
                        await session.execute(
                            text("SELECT id, slug, human_key FROM projects WHERE slug = :k OR human_key = :k"),
                            {"k": project},
                        )
                    ).fetchone()
                    if not prow:
                        raise HTTPException(status_code=404, detail="Project not found")

                    # Extract project info consistently
                    project_id = int(prow[0])
                    project_slug = prow[1]
                    project_human_key = prow[2]

                    # Get or create "HumanOverseer" agent (with race condition protection)
                    overseer_row = (
                        await session.execute(
                            text("SELECT id, name FROM agents WHERE project_id = :pid AND name = :name"),
                            {"pid": project_id, "name": overseer_name}
                        )
                    ).fetchone()

                    if not overseer_row:
                        # Create HumanOverseer agent (use INSERT OR IGNORE to handle race conditions)
                        await session.execute(
                            text("""
                                INSERT OR IGNORE INTO agents (
                                    project_id,
                                    name,
                                    program,
                                    model,
                                    task_description,
                                    contact_policy,
                                    attachments_policy,
                                    inception_ts,
                                    last_active_ts
                                )
                                VALUES (
                                    :pid,
                                    :name,
                                    :program,
                                    :model,
                                    :task,
                                    :policy,
                                    :attachments_policy,
                                    :ts,
                                    :ts
                                )
                            """),
                            {
                                "pid": project_id,
                                "name": overseer_name,
                                "program": "WebUI",
                                "model": "Human",
                                "task": "Human operator providing guidance and oversight to agents",
                                "policy": "open",
                                "attachments_policy": "auto",
                                # Use naive UTC datetime for SQLite compatibility
                                "ts": datetime.now(timezone.utc).replace(tzinfo=None),
                            },
                        )
                        # Fetch the agent (whether we just created it or another request did)
                        overseer_row = (
                            await session.execute(
                                text("SELECT id, name FROM agents WHERE project_id = :pid AND name = :name"),
                                {"pid": project_id, "name": overseer_name}
                            )
                        ).fetchone()

                        if not overseer_row:
                            raise HTTPException(status_code=500, detail="Failed to create HumanOverseer agent")

                    # Extract overseer_id for later use
                    overseer_id = overseer_row[0]

                    result = await session.execute(
                        text("""
                            INSERT INTO messages (project_id, sender_id, subject, body_md, importance, thread_id, created_ts, ack_required)
                            VALUES (:pid, :sid, :subj, :body, :imp, :tid, :ts, :ack)
                            RETURNING id
                        """),
                        {
                            "pid": project_id,
                            "sid": overseer_id,
                            "subj": subject,
                            "body": full_body,
                            "imp": "high",  # Always high importance for overseer
                            "tid": thread_id,
                            "ts": now,
                            "ack": False
                        }
                    )
                    message_row = result.fetchone()
                    if not message_row:
                        raise HTTPException(status_code=500, detail="Failed to create message")
                    message_id = message_row[0]

                    # Insert recipients (optimized: bulk SELECT + bulk INSERT instead of N+1 queries)
                    # Build SQL with proper parameter expansion for IN clause
                    placeholders = ", ".join([f":name_{i}" for i in range(len(recipients))])
                    params: dict[str, Any] = {"pid": project_id}
                    params.update({f"name_{i}": name for i, name in enumerate(recipients)})

                    # Single query to get all valid recipient IDs
                    recipient_rows = await session.execute(
                        text(f"SELECT id, name FROM agents WHERE project_id = :pid AND name IN ({placeholders})"),
                        params
                    )
                    recipient_map = {row[1]: row[0] for row in recipient_rows.fetchall()}  # name -> id mapping

                    # Build valid recipients list (only those that exist)
                    valid_recipients = [name for name in recipients if name in recipient_map]

                    # Bulk insert all message_recipients (single executemany call)
                    if valid_recipients:
                        # Prepare bulk insert params
                        insert_params = [
                            {"mid": message_id, "aid": recipient_map[name], "kind": "to"}
                            for name in valid_recipients
                        ]
                        # Use executemany for bulk insert
                        await session.execute(
                            text("""
                                INSERT INTO message_recipients (message_id, agent_id, kind)
                                VALUES (:mid, :aid, :kind)
                            """),
                            insert_params
                        )

                    # If no valid recipients found, rollback and error
                    if not valid_recipients:
                        await session.rollback()
                        raise HTTPException(
                            status_code=400,
                            detail=f"None of the specified recipients exist in this project. Available agents can be seen at /mail/{project_slug}"
                        )

                    # Update HumanOverseer activity timestamp before commit.
                    await session.execute(
                        text("UPDATE agents SET last_active_ts = :ts WHERE id = :id"),
                        {"ts": now, "id": overseer_id}
                    )

                    await session.commit()

                from .storage import ensure_archive, write_message_bundle

                settings = get_settings()
                archive = await ensure_archive(settings, project_slug)
                message_dict = {
                    "id": message_id,
                    "thread_id": thread_id,
                    "project": project_human_key,
                    "project_slug": project_slug,
                    "from": overseer_name,
                    "to": valid_recipients,
                    "cc": [],
                    "bcc": [],
                    "subject": subject,
                    "importance": "high",
                    "ack_required": False,
                    "created": now.isoformat(),
                    "attachments": [],
                }

                try:
                    async with archive_write_lock(archive):
                        await write_message_bundle(
                            archive,
                            message_dict,
                            full_body,
                            overseer_name,
                            valid_recipients,
                            extra_paths=None,
                            commit_text=f"Human Overseer message: {subject}",
                            sender_outbox_name=overseer_name,
                        )
                except Exception as git_error:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to write message to Git archive: {git_error!s}"
                    ) from git_error

                assert message_id is not None
                await _record_event(
                    project_id,
                    "message.sent",
                    overseer_name,
                    _message_sent_event_payload(
                        message_id, thread_id, subject, "high", False, project_slug, valid_recipients, [], []
                    ),
                )
                await audit.record(
                    "overseer.sent", actor=await _actor(request), target_type="message", target_id=str(message_id),
                    details=f"project={project_slug}; to={','.join(valid_recipients)}; subject={subject}"[:2048],
                    request=request,
                )

                return JSONResponse({
                    "success": True,
                    "message_id": message_id,
                    "recipients": valid_recipients,
                    "sent_at": now.isoformat()
                })

            except HTTPException:
                raise
            except Exception as e:
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"Failed to send message: {e!s}") from e

        # ========== Archive Visualization Routes ==========

        def _validate_project_slug(slug: str) -> bool:
            """Validate project slug format to prevent path traversal."""

            # Slugs should only contain lowercase letters, numbers, hyphens, underscores
            # No path separators or relative path components
            if not slug:
                return False
            if slug in (".", "..", "/", "\\"):
                return False
            if "/" in slug or "\\" in slug or ".." in slug:
                return False
            # Should match safe slug pattern
            return bool(_SLUG_VALIDATOR_RE.match(slug))

        @fastapi_app.get("/mail/archive/guide", response_class=HTMLResponse)
        async def archive_guide() -> HTMLResponse:
            """Display the archive access guide and overview."""
            settings = get_settings()
            guide_stats = await asyncio.to_thread(_collect_archive_guide_stats_sync, settings)

            # Get list of projects for picker
            async with get_session() as session:
                rows = await session.execute(text("SELECT slug, human_key FROM projects ORDER BY human_key"))
                projects = [{"slug": r[0], "human_key": r[1]} for r in rows.fetchall()]

            return await _render(
                "archive_guide.html",
                storage_root=guide_stats["storage_root"],
                total_commits=guide_stats["total_commits"],
                project_count=guide_stats["project_count"],
                repo_size=guide_stats["repo_size"],
                last_commit_time=guide_stats["last_commit_time"],
                projects=projects,
            )

        @fastapi_app.get("/mail/archive/activity", response_class=HTMLResponse)
        async def archive_activity(limit: int = 50) -> HTMLResponse:
            """Display recent commits across all projects."""
            # Validate and cap limit to prevent DoS
            limit = max(1, min(limit, 500))  # Between 1 and 500

            settings = get_settings()
            repo_root = await asyncio.to_thread(_expanduser_resolve_path, Path(settings.storage.root))
            if not await asyncio.to_thread(_path_exists, repo_root / ".git"):
                return await _render("archive_activity.html", commits=[])

            repo = None
            try:
                repo = await asyncio.to_thread(_open_git_repo, repo_root)
                commits = await get_recent_commits(repo, limit=limit)
                return await _render("archive_activity.html", commits=commits)
            finally:
                if repo is not None:
                    await asyncio.to_thread(repo.close)

        @fastapi_app.get("/mail/archive/commit/{sha}", response_class=HTMLResponse)
        async def archive_commit(sha: str) -> HTMLResponse:
            """Display detailed commit information with diffs."""
            settings = get_settings()
            repo_root = await asyncio.to_thread(_expanduser_resolve_path, Path(settings.storage.root))
            if not await asyncio.to_thread(_path_exists, repo_root / ".git"):
                return await _render("error.html", message="Archive repository not found")

            repo = None
            try:
                repo = await asyncio.to_thread(_open_git_repo, repo_root)
                commit = await get_commit_detail(repo, sha)
                return await _render("archive_commit.html", commit=commit)
            except ValueError:
                # Validation errors (bad SHA, etc.)
                return await _render("error.html", message="Invalid commit identifier")
            except Exception:
                # Don't leak error details
                return await _render("error.html", message="Commit not found")
            finally:
                if repo is not None:
                    await asyncio.to_thread(repo.close)

        @fastapi_app.get("/mail/archive/timeline", response_class=HTMLResponse)
        async def archive_timeline(project: str | None = None) -> HTMLResponse:
            """Display communication timeline with Mermaid.js visualization."""
            # Validate project slug if provided
            if project and not _validate_project_slug(project):
                return await _render("error.html", message="Invalid project identifier")

            settings = get_settings()
            repo_root = await asyncio.to_thread(_expanduser_resolve_path, Path(settings.storage.root))
            if not await asyncio.to_thread(_path_exists, repo_root / ".git"):
                return await _render("error.html", message="Archive repository not found")

            # Default to first project if not specified
            if not project:
                async with get_session() as session:
                    row = (
                        await session.execute(text("SELECT slug, human_key FROM projects ORDER BY id LIMIT 1"))
                    ).fetchone()
                    if row:
                        project = row[0]
                    else:
                        return await _render("error.html", message="No projects found")

            # Get project name
            project_name = project
            async with get_session() as session:
                row = (
                    await session.execute(text("SELECT human_key FROM projects WHERE slug = :s"), {"s": project})
                ).fetchone()
                if row:
                    project_name = row[0]

            repo = None
            try:
                repo = await asyncio.to_thread(_open_git_repo, repo_root)
                commits = await get_timeline_commits(repo, project, limit=100)
                return await _render("archive_timeline.html", commits=commits, project=project, project_name=project_name)
            finally:
                if repo is not None:
                    await asyncio.to_thread(repo.close)

        @fastapi_app.get("/mail/archive/browser", response_class=HTMLResponse)
        async def archive_browser(project: str | None = None, path: str = "") -> HTMLResponse:
            """Browse archive files and directories."""
            if not project:
                # Show project selector - requires project parameter
                return await _render("error.html", message="Please select a project to browse")

            # Validate project slug
            if not _validate_project_slug(project):
                return await _render("error.html", message="Invalid project identifier")

            settings = get_settings()
            archive = await _open_existing_project_archive(settings, project)
            if archive is None:
                return await _render("error.html", message="Project archive not found")
            try:
                tree = await get_archive_tree(archive, path)
                return await _render("archive_browser.html", tree=tree, project=project, path=path)
            except ValueError:
                return await _render("error.html", message="Invalid archive path")
            finally:
                await asyncio.to_thread(archive.repo.close)

        @fastapi_app.get("/mail/archive/browser/{project}/file")
        async def archive_browser_file(project: str, path: str) -> JSONResponse:
            """Get file content from archive."""
            # Validate project slug
            if not _validate_project_slug(project):
                raise HTTPException(status_code=400, detail="Invalid project identifier")

            try:
                settings = get_settings()
                archive = await _open_existing_project_archive(settings, project)
                if archive is None:
                    raise HTTPException(status_code=404, detail="Project archive not found")
                try:
                    content = await get_file_content(archive, path)
                finally:
                    await asyncio.to_thread(archive.repo.close)

                if content is None:
                    raise HTTPException(status_code=404, detail="File not found")

                return JSONResponse(content=content)
            except ValueError as err:
                # Path validation errors
                raise HTTPException(status_code=400, detail="Invalid file path") from err
            except HTTPException:
                raise
            except Exception as err:
                raise HTTPException(status_code=404, detail="File not found") from err

        @fastapi_app.get("/mail/archive/browser/{project}/download")
        async def archive_browser_download(project: str, path: str) -> Response:
            """Download a file from the archive as an attachment (#221)."""
            # Validate project slug
            if not _validate_project_slug(project):
                raise HTTPException(status_code=400, detail="Invalid project identifier")

            try:
                settings = get_settings()
                archive = await _open_existing_project_archive(settings, project)
                if archive is None:
                    raise HTTPException(status_code=404, detail="Project archive not found")
                try:
                    content = await get_file_content(archive, path)
                finally:
                    await asyncio.to_thread(archive.repo.close)

                if content is None:
                    raise HTTPException(status_code=404, detail="File not found")

                # Derive a safe download filename from the (already validated)
                # path's basename; strip any directory components and quotes.
                filename = PurePosixPath(path.replace("\\", "/")).name or "download"
                filename = filename.replace('"', "").replace("\r", "").replace("\n", "")
                return Response(
                    content=content,
                    media_type="application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                )
            except ValueError as err:
                # Path validation errors
                raise HTTPException(status_code=400, detail="Invalid file path") from err
            except HTTPException:
                raise
            except Exception as err:
                raise HTTPException(status_code=404, detail="File not found") from err

        @fastapi_app.get("/mail/archive/network", response_class=HTMLResponse)
        async def archive_network(project: str | None = None) -> HTMLResponse:
            """Display agent communication network graph."""
            # Validate project slug if provided
            if project and not _validate_project_slug(project):
                return await _render("error.html", message="Invalid project identifier")

            settings = get_settings()
            repo_root = await asyncio.to_thread(_expanduser_resolve_path, Path(settings.storage.root))
            if not await asyncio.to_thread(_path_exists, repo_root / ".git"):
                return await _render("error.html", message="Archive repository not found")

            # Default to first project
            if not project:
                async with get_session() as session:
                    row = (
                        await session.execute(text("SELECT slug, human_key FROM projects ORDER BY id LIMIT 1"))
                    ).fetchone()
                    if row:
                        project = row[0]
                    else:
                        return await _render("error.html", message="No projects found")

            # Get project name
            project_name = project
            async with get_session() as session:
                row = (
                    await session.execute(text("SELECT human_key FROM projects WHERE slug = :s"), {"s": project})
                ).fetchone()
                if row:
                    project_name = row[0]

            repo = None
            try:
                repo = await asyncio.to_thread(_open_git_repo, repo_root)
                graph = await get_agent_communication_graph(repo, project, limit=200)
                return await _render("archive_network.html", graph=graph, project=project, project_name=project_name)
            finally:
                if repo is not None:
                    await asyncio.to_thread(repo.close)

        @fastapi_app.get("/mail/api/projects/{project}/agents")
        async def api_project_agents(project: str) -> JSONResponse:
            """Get list of agents for a project."""
            # Validate project slug
            if not _validate_project_slug(project):
                raise HTTPException(status_code=400, detail="Invalid project identifier")

            async with get_session() as session:
                # Get project ID
                proj_result = await session.execute(
                    text("SELECT id FROM projects WHERE slug = :k OR human_key = :k"),
                    {"k": project}
                )
                prow = proj_result.fetchone()
                if not prow:
                    raise HTTPException(status_code=404, detail="Project not found")

                # Get agents for this project
                agents_result = await session.execute(
                    text("SELECT name FROM agents WHERE project_id = :pid ORDER BY name"),
                    {"pid": prow[0]}
                )
                agents = [r[0] for r in agents_result.fetchall()]

            return JSONResponse({"agents": agents})

        @fastapi_app.get("/mail/archive/time-travel", response_class=HTMLResponse)
        async def archive_time_travel() -> HTMLResponse:
            """Display time-travel interface."""
            # Get all projects
            async with get_session() as session:
                rows = await session.execute(text("SELECT slug FROM projects ORDER BY human_key"))
                projects = [r[0] for r in rows.fetchall()]

            return await _render("archive_time_travel.html", projects=projects)

        @fastapi_app.get("/mail/archive/time-travel/snapshot")
        async def archive_time_travel_snapshot(project: str, agent: str, timestamp: str) -> JSONResponse:
            """Get historical inbox snapshot."""
            # Validate project slug
            if not _validate_project_slug(project):
                raise HTTPException(status_code=400, detail="Invalid project identifier")

            # Validate agent name (alphanumeric only)
            if not agent or not _AGENT_NAME_VALIDATOR_RE.match(agent):
                raise HTTPException(status_code=400, detail="Invalid agent name format")

            # Validate timestamp format (basic ISO 8601 check)
            if not timestamp or not _TIMESTAMP_VALIDATOR_RE.match(timestamp):
                raise HTTPException(status_code=400, detail="Invalid timestamp format. Use ISO 8601 format (YYYY-MM-DDTHH:MM)")

            try:
                # Get project archive
                settings = get_settings()
                repo = await _open_existing_project_archive(settings, project)
                if repo is None:
                    return JSONResponse({
                        "messages": [],
                        "snapshot_time": None,
                        "commit_sha": None,
                        "requested_time": timestamp,
                        "error": "Project archive not found",
                    })

                try:
                    # Get historical snapshot
                    snapshot = await get_historical_inbox_snapshot(repo, agent, timestamp, limit=200)
                    return JSONResponse(snapshot)
                finally:
                    await asyncio.to_thread(repo.repo.close)

            except Exception as e:
                # Log error but return empty result rather than failing
                structlog.get_logger("archive").warning(
                    "time_travel_failed",
                    project=project,
                    agent=agent,
                    timestamp=timestamp,
                    error=str(e)
                )
                return JSONResponse({
                    "messages": [],
                    "snapshot_time": None,
                    "commit_sha": None,
                    "requested_time": timestamp,
                    "error": f"Unable to retrieve historical snapshot: {e!s}"
                })


    try:
        _register_mail_ui()
    except Exception as exc:
        # templates/Jinja may be missing in some environments; UI remains optional
        with contextlib.suppress(Exception):
            structlog.get_logger("ui").error("ui_init_failed", error=str(exc))
        pass

    # Keep the auto-generated /openapi.json focused on the real API contract.
    # The browser-facing SSR mail UI (and its UI-backing JSON helpers) all live
    # under the `/mail` prefix; they are registered for humans, not as part of
    # the documented API surface, so we filter them out of the schema. The
    # routes stay fully registered and functional — they are only omitted from
    # the OpenAPI document. Using a custom app.openapi() (rather than
    # include_in_schema=False on ~33 decorators) keeps this in one place and
    # automatically covers any future /mail/* routes.
    from fastapi.openapi.utils import get_openapi as _get_openapi

    def _custom_openapi() -> dict[str, Any]:
        if fastapi_app.openapi_schema:
            return fastapi_app.openapi_schema
        schema = _get_openapi(
            title=fastapi_app.title,
            version=fastapi_app.version,
            openapi_version=fastapi_app.openapi_version,
            description=fastapi_app.description,
            routes=fastapi_app.routes,
        )
        paths = schema.get("paths")
        if isinstance(paths, dict):
            schema["paths"] = {
                path: item
                for path, item in paths.items()
                if not (path == "/mail" or path.startswith("/mail/"))
            }
        fastapi_app.openapi_schema = schema
        return schema

    # Install the custom generator (FastAPI's documented extension point for
    # overriding the OpenAPI document); cast keeps the bound-method override
    # explicit for the type checker.
    cast(Any, fastapi_app).openapi = _custom_openapi

    # Static web UI (SPA) routing support
    def _resolve_web_root() -> Path | None:
        candidates: list[Path] = []
        with contextlib.suppress(Exception):
            candidates.append(Path(__file__).resolve().parents[3] / "web")
        candidates.append(Path.cwd() / "web")
        for candidate in candidates:
            try:
                if candidate.exists() and (candidate / "index.html").exists():
                    return candidate
            except Exception:
                continue
        return None

    web_root = _resolve_web_root()
    if web_root is not None:
        fastapi_app.mount("/", StaticFiles(directory=str(web_root), html=True), name="web")

        def _is_api_path(path: str) -> bool:
            if base_no_slash == "/":
                return True
            return path == base_no_slash or path.startswith(base_no_slash + "/")

        def _should_spa_fallback(path: str) -> bool:
            if _is_api_path(path):
                return False
            return not (path == "/mail" or path.startswith("/mail/"))

        @fastapi_app.exception_handler(HTTPException)
        async def spa_fallback(request: Request, exc: HTTPException):
            if exc.status_code == status.HTTP_404_NOT_FOUND and _should_spa_fallback(request.url.path):
                return FileResponse(web_root / "index.html")
            return await http_exception_handler(request, exc)

    return fastapi_app


def main() -> None:
    """Run the HTTP transport using settings-specified host/port."""

    parser = argparse.ArgumentParser(description="Run the MCP Agent Mail HTTP transport")
    parser.add_argument("--host", help="Override HTTP host", default=None)
    parser.add_argument("--port", help="Override HTTP port", type=int, default=None)
    parser.add_argument("--log-level", help="Uvicorn log level", default="info")
    # Be tolerant of extraneous argv when invoked under test runners
    args, _unknown = parser.parse_known_args()

    settings = get_settings()
    host = args.host or settings.http.host
    port = args.port or settings.http.port

    app = build_http_app(settings)
    # Disable WebSockets when running the service directly; HTTP-only transport
    import inspect as _inspect

    _sig = _inspect.signature(uvicorn.run)
    _kwargs: dict[str, Any] = {"host": host, "port": port, "log_level": args.log_level}
    if "ws" in _sig.parameters:
        _kwargs["ws"] = "none"
    uvicorn.run(app, **_kwargs)


if __name__ == "__main__":  # pragma: no cover - manual execution path
    main()
