"""People's credentials: web-UI password accounts with mandatory MFA, and personal access tokens.

Personal access tokens (bottom of this module) are what a person's agents use
instead of the shared server token: minted for one person and one machine via
`mail_wake.py login`, stored hashed, expiring and individually revocable.

Password accounts are for people without a GitHub account (GitHub sign-in
lives in http.py). The pieces are standard and use only the standard library:

- passwords: scrypt with a per-user random salt, checked off the event loop;
  unknown usernames are checked against a dummy hash so timing does not reveal
  which accounts exist
- second factor: RFC 6238 TOTP (SHA-1, 30 s, 6 digits; what Google
  Authenticator, 1Password, Authy etc. use), +/- one step of clock drift, and
  a code's time step can't be used twice
- 10 one-time recovery codes, stored hashed
- 5 failed attempts (password or code) lock the account for 15 minutes

Accounts are created by the server's operator (`mcp-agent-mail users add`);
enrolment of the authenticator happens at the first sign-in.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import re
import secrets
import struct
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from sqlmodel import select

from .db import ensure_schema, get_session
from .models import ApiToken, UiSession, UiUser

# A plain name or an email address (stored lower-case), e.g. alice or peter.richards@example.com.
USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._@+-]{0,63}$")
USERNAME_RULE = "Username must be 1-64 characters: lowercase letters, digits, '.', '_', '-', '+' or '@' (an email address works)."
MIN_PASSWORD_LENGTH = 12
MAX_FAILED_ATTEMPTS = 5
LOCKOUT = timedelta(minutes=15)
TOTP_PERIOD = 30
TOTP_DIGITS = 6
RECOVERY_CODE_COUNT = 10
ISSUER = "Agent Mail"
# A session user for a password account; ":" can't appear in a GitHub login.
SESSION_PREFIX = "local:"

_SCRYPT_N, _SCRYPT_R, _SCRYPT_P = 2**14, 8, 1
_dummy_hash: str | None = None


class AuthError(Exception):
    """A sign-in step failed; the message is safe to show the person signing in.

    ``locked`` is true when this failure is the one that locked the account.
    """

    def __init__(self, message: str, *, locked: bool = False) -> None:
        super().__init__(message)
        self.locked = locked


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------- passwords


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=32)
    b64 = base64.b64encode
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${b64(salt).decode()}${b64(digest).decode()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt, digest = stored.split("$")
        if scheme != "scrypt":
            return False
        actual = hashlib.scrypt(
            password.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p), dklen=len(base64.b64decode(digest))
        )
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(actual, base64.b64decode(digest))


def check_new_password(password: str) -> None:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters.")


# ---------------------------------------------------------------- TOTP + recovery codes


def new_totp_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")


def totp_code(secret: str, step: int) -> str:
    key = base64.b32decode(secret + "=" * (-len(secret) % 8), casefold=True)
    mac = hmac.new(key, struct.pack(">Q", step), hashlib.sha1).digest()
    offset = mac[-1] & 0x0F
    value = struct.unpack(">I", mac[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(value % 10**TOTP_DIGITS).zfill(TOTP_DIGITS)


def _matching_step(secret: str, code: str, last_step: int, now: float | None = None) -> int | None:
    """The time step `code` belongs to (allowing one step of drift), unless already used."""
    code = code.strip().replace(" ", "")
    if not (code.isascii() and code.isdigit() and len(code) == TOTP_DIGITS):  # isdigit alone admits e.g. Arabic-Indic digits
        return None
    current = int((time.time() if now is None else now) // TOTP_PERIOD)
    for step in (current, current - 1, current + 1):
        if step > last_step and hmac.compare_digest(totp_code(secret, step), code):
            return step
    return None


def otpauth_uri(secret: str, username: str) -> str:
    label = urllib.parse.quote(f"{ISSUER}:{username}")
    query = urllib.parse.urlencode({"secret": secret, "issuer": ISSUER, "digits": TOTP_DIGITS, "period": TOTP_PERIOD})
    return f"otpauth://totp/{label}?{query}"


def _normalize_recovery_code(code: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", code.upper())


def _hash_recovery_code(code: str) -> str:
    return hashlib.sha256(_normalize_recovery_code(code).encode()).hexdigest()


def new_recovery_codes() -> tuple[list[str], list[str]]:
    """(codes to show once, hashes to store)."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    codes = []
    for _ in range(RECOVERY_CODE_COUNT):
        raw = "".join(secrets.choice(alphabet) for _ in range(10))
        codes.append(f"{raw[:5]}-{raw[5:]}")
    return codes, [_hash_recovery_code(c) for c in codes]


# ---------------------------------------------------------------- account store


async def get_user(username: str) -> UiUser | None:
    await ensure_schema()
    async with get_session() as session:
        return (await session.execute(select(UiUser).where(UiUser.username == username.strip().lower()))).scalars().first()


async def get_user_by_id(user_id: int) -> UiUser | None:
    await ensure_schema()
    async with get_session() as session:
        return await session.get(UiUser, user_id)


async def has_users() -> bool:
    await ensure_schema()
    async with get_session() as session:
        return (await session.execute(select(UiUser.id).where(cast(Any, UiUser.disabled).is_(False)).limit(1))).first() is not None


async def list_users() -> list[UiUser]:
    await ensure_schema()
    async with get_session() as session:
        return list((await session.execute(select(UiUser).order_by(cast(Any, UiUser.username)))).scalars().all())


def normalize_username(username: str) -> str:
    """The stored form of a new username (trimmed, lower-case); ValueError if it isn't allowed."""
    username = username.strip().lower()
    if not USERNAME_RE.match(username):
        raise ValueError(USERNAME_RULE)
    return username


async def create_user(username: str, password: str) -> UiUser:
    username = normalize_username(username)
    check_new_password(password)
    if await get_user(username) is not None:
        raise ValueError(f"User {username} already exists.")
    user = UiUser(username=username, password_hash=await asyncio.to_thread(hash_password, password))
    async with get_session() as session:
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


async def _update(username: str, **changes: Any) -> UiUser:
    await ensure_schema()
    async with get_session() as session:
        user = (await session.execute(select(UiUser).where(UiUser.username == username.strip().lower()))).scalars().first()
        if user is None:
            raise ValueError(f"No user named {username}.")
        for key, value in changes.items():
            setattr(user, key, value)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


async def set_password(username: str, password: str) -> UiUser:
    """Replace the password and end the account's sessions (a changed password should lock others out)."""
    check_new_password(password)
    user = await _update(
        username, password_hash=await asyncio.to_thread(hash_password, password), failed_attempts=0, locked_until=None
    )
    await revoke_all_sessions(f"{SESSION_PREFIX}{user.username}")
    return user


async def reset_mfa(username: str) -> UiUser:
    """Forget the authenticator and recovery codes; the next sign-in enrols again."""
    return await _update(username, totp_secret=None, totp_pending_secret=None, totp_last_step=0, recovery_code_hashes=[])


async def set_disabled(username: str, disabled: bool) -> UiUser:
    """Block or unblock sign-in; disabling also ends the account's open sessions at once."""
    user = await _update(username, disabled=disabled, failed_attempts=0, locked_until=None)
    if disabled:
        await revoke_all_sessions(f"{SESSION_PREFIX}{user.username}")
    return user


# ---------------------------------------------------------------- sign-in steps


async def _record_failure(user_id: int) -> bool:
    """Count a failed attempt; True when it locked the account."""
    async with get_session() as session:
        user = await session.get(UiUser, user_id)
        if user is None:
            return False
        user.failed_attempts += 1
        locked = user.failed_attempts >= MAX_FAILED_ATTEMPTS
        if locked:
            user.locked_until = _now() + LOCKOUT
            user.failed_attempts = 0
        session.add(user)
        await session.commit()
        return locked


def _ensure_usable(user: UiUser) -> None:
    if user.disabled:
        raise AuthError("Wrong username or password.")
    if user.locked_until and user.locked_until > _now():
        minutes = max(1, int((user.locked_until - _now()).total_seconds() // 60) + 1)
        raise AuthError(f"Too many failed attempts. Try again in {minutes} minute{'s' if minutes != 1 else ''}.")


async def check_password(username: str, password: str) -> UiUser:
    """First factor. Raises AuthError (with the same message for unknown users and wrong passwords)."""
    global _dummy_hash
    user = await get_user(username.strip())
    if user is None:
        if _dummy_hash is None:
            _dummy_hash = await asyncio.to_thread(hash_password, secrets.token_hex(16))
        await asyncio.to_thread(verify_password, password, _dummy_hash)  # same cost as a real check
        raise AuthError("Wrong username or password.")
    _ensure_usable(user)
    if not await asyncio.to_thread(verify_password, password, user.password_hash):
        assert user.id is not None
        raise AuthError("Wrong username or password.", locked=await _record_failure(user.id))
    return user


async def begin_enrollment(user_id: int) -> tuple[UiUser, str]:
    """The authenticator secret to show for enrolment (stable across page reloads)."""
    async with get_session() as session:
        user = await session.get(UiUser, user_id)
        if user is None or user.disabled:
            raise AuthError("Sign-in expired. Please start again.")
        secret = user.totp_pending_secret
        if not secret:
            secret = user.totp_pending_secret = new_totp_secret()
            session.add(user)
            await session.commit()
            await session.refresh(user)
        return user, secret


async def complete_enrollment(user_id: int, code: str) -> tuple[UiUser, list[str]]:
    """Confirm the authenticator with a code; returns the recovery codes to show once."""
    async with get_session() as session:
        user = await session.get(UiUser, user_id)
        if user is None or user.disabled or not user.totp_pending_secret:
            raise AuthError("Sign-in expired. Please start again.")
        _ensure_usable(user)
        step = _matching_step(user.totp_pending_secret, code, 0)
        if step is None:
            raise AuthError("That code didn't match. Check the time on your device and try the newest code.")
        codes, hashes = new_recovery_codes()
        user.totp_secret, user.totp_pending_secret = user.totp_pending_secret, None
        user.totp_last_step, user.recovery_code_hashes = step, hashes
        user.failed_attempts, user.last_login_ts = 0, _now()
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user, codes


async def check_second_factor(user_id: int, code: str) -> UiUser:
    """Second factor: an authenticator code, or one of the recovery codes (used up)."""
    async with get_session() as session:
        user = await session.get(UiUser, user_id)
        if user is None or not user.totp_secret:
            raise AuthError("Sign-in expired. Please start again.")
        _ensure_usable(user)
        step = _matching_step(user.totp_secret, code, user.totp_last_step)
        if step is not None:
            user.totp_last_step = step
        else:
            recovery = _hash_recovery_code(code)
            if not _normalize_recovery_code(code) or recovery not in user.recovery_code_hashes:
                await session.rollback()  # expires `user`; use the id we were given
                raise AuthError("That code didn't match.", locked=await _record_failure(user_id))
            user.recovery_code_hashes = [h for h in user.recovery_code_hashes if h != recovery]
        user.failed_attempts, user.last_login_ts = 0, _now()
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


# ---------------------------------------------------------------- personal access tokens

TOKEN_PREFIX = "amt_"  # recognisable in configs, logs and secret scanners
API_TOKEN_LIFETIME = timedelta(days=90)
_TOKEN_CACHE_SECONDS = 60.0  # per worker: a revocation elsewhere takes effect within this
_LAST_USED_RESOLUTION = timedelta(minutes=5)


@dataclass(frozen=True)
class TokenIdentity:
    """Who a valid personal access token speaks for, and the agent it was issued for, if any (#42)."""

    owner: str
    project_key: str | None = None
    agent_name: str | None = None


_token_cache: dict[str, tuple[TokenIdentity | None, float]] = {}


def _hash_api_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def create_api_token(
    owner: str,
    name: str,
    lifetime: timedelta = API_TOKEN_LIFETIME,
    *,
    project_key: str | None = None,
    agent_name: str | None = None,
) -> tuple[str, ApiToken]:
    """Mint a token for `owner`; returns (the token, shown once, never stored; its record).

    With `project_key` and `agent_name`, the token is bound to that agent: MCP calls carrying
    it act as the agent, when the agent is `owner`'s, without its registration token (#42).
    """
    token = f"{TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
    bound = bool(project_key and agent_name)
    row = ApiToken(
        owner=owner[:128],
        name=(name.strip() or "unnamed")[:128],
        token_hash=_hash_api_token(token),
        prefix=token[: len(TOKEN_PREFIX) + 6],
        expires_ts=_now() + lifetime,
        project_key=project_key[:255] if bound and project_key else None,
        agent_name=agent_name[:128] if bound and agent_name else None,
    )
    await ensure_schema()
    async with get_session() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return token, row


async def list_api_tokens(owner: str) -> list[ApiToken]:
    await ensure_schema()
    async with get_session() as session:
        return list(
            (
                await session.execute(
                    select(ApiToken).where(ApiToken.owner == owner).order_by(cast(Any, ApiToken.created_ts).desc())
                )
            ).scalars().all()
        )


async def revoke_api_token(owner: str, token_id: int) -> bool:
    """Revoke one of `owner`'s tokens. False if it isn't theirs or doesn't exist."""
    await ensure_schema()
    async with get_session() as session:
        row = await session.get(ApiToken, token_id)
        if row is None or row.owner != owner:
            return False
        if row.revoked_ts is None:
            row.revoked_ts = _now()
            session.add(row)
            await session.commit()
        _token_cache.pop(row.token_hash, None)
        return True


async def api_token_identity(token: str) -> TokenIdentity | None:
    """Who a presented token speaks for, or None if it is unknown, revoked or expired."""
    if not token.startswith(TOKEN_PREFIX):
        return None
    token_hash = _hash_api_token(token)
    cached = _token_cache.get(token_hash)
    if cached is not None and time.monotonic() - cached[1] < _TOKEN_CACHE_SECONDS:
        return cached[0]
    await ensure_schema()
    identity: TokenIdentity | None = None
    async with get_session() as session:
        row = (await session.execute(select(ApiToken).where(ApiToken.token_hash == token_hash))).scalars().first()
        now = _now()
        if row is not None and row.revoked_ts is None and row.expires_ts > now:
            identity = TokenIdentity(row.owner, row.project_key, row.agent_name)
            if row.last_used_ts is None or now - row.last_used_ts > _LAST_USED_RESOLUTION:
                row.last_used_ts = now
                session.add(row)
                await session.commit()
    _token_cache[token_hash] = (identity, time.monotonic())
    return identity


# ---------------------------------------------------------------- web sessions
# Pattern from lfr-tunnel's portal_session_store: the cookie carries a random
# token, the database keeps its hash (a stolen row is useless), and an
# in-memory cache saves a query per request. Revoking clears this worker's cache
# at once; other workers notice within _SESSION_CACHE_SECONDS.

SESSION_IDLE_LIMIT = timedelta(days=30)
SESSION_MAX_AGE = timedelta(days=90)
_SESSION_TOUCH = timedelta(hours=1)  # how often activity is written back
_SESSION_CACHE_SECONDS = 30.0
_session_cache: dict[str, tuple[UiSession | None, float]] = {}
_account_cache: dict[str, tuple[bool, float]] = {}


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _session_live(row: UiSession, now: datetime) -> bool:
    return (
        row.revoked_ts is None
        and now - row.created_ts < SESSION_MAX_AGE
        and now - row.last_seen_ts < SESSION_IDLE_LIMIT
    )


async def create_session(user: str, method: str, *, ip: str = "", user_agent: str = "") -> str:
    """Start a session for `user`; returns the cookie value (shown to the browser only)."""
    token = secrets.token_urlsafe(32)
    await ensure_schema()
    async with get_session() as session:
        session.add(
            UiSession(
                token_hash=_hash_session_token(token), user=user[:128], method=method[:16],
                ip=ip[:64], user_agent=user_agent[:200],
            )
        )
        await session.commit()
    return token


async def lookup_session(token: str, *, activity: bool) -> UiSession | None:
    """The live session for a cookie value, or None. `activity` slides its idle expiry forward."""
    if not token:
        return None
    token_hash = _hash_session_token(token)
    now = _now()
    cached = _session_cache.get(token_hash)
    if cached is not None and time.monotonic() - cached[1] < _SESSION_CACHE_SECONDS:
        row = cached[0]
    else:
        await ensure_schema()
        async with get_session() as session:
            row = (await session.execute(select(UiSession).where(UiSession.token_hash == token_hash))).scalars().first()
        _session_cache[token_hash] = (row, time.monotonic())
    if row is None or not _session_live(row, now):
        return None
    if activity and now - row.last_seen_ts > _SESSION_TOUCH:
        async with get_session() as session:
            db_row = await session.get(UiSession, row.id)
            if db_row is not None and db_row.revoked_ts is None:
                db_row.last_seen_ts = now
                session.add(db_row)
                await session.commit()
                await session.refresh(db_row)
                row = db_row
                _session_cache[token_hash] = (row, time.monotonic())
    return row


async def local_account_active(username: str) -> bool:
    """Whether a password account exists and isn't disabled (cached like sessions)."""
    cached = _account_cache.get(username)
    if cached is not None and time.monotonic() - cached[1] < _SESSION_CACHE_SECONDS:
        return cached[0]
    user = await get_user(username)
    active = user is not None and not user.disabled
    _account_cache[username] = (active, time.monotonic())
    return active


async def list_sessions(user: str) -> list[UiSession]:
    await ensure_schema()
    now = _now()
    async with get_session() as session:
        rows = (
            await session.execute(
                select(UiSession).where(UiSession.user == user).order_by(cast(Any, UiSession.last_seen_ts).desc())
            )
        ).scalars().all()
    return [row for row in rows if _session_live(row, now)]


async def _revoke(where: Any) -> int:
    await ensure_schema()
    async with get_session() as session:
        rows = (
            await session.execute(select(UiSession).where(where, cast(Any, UiSession.revoked_ts).is_(None)))
        ).scalars().all()
        for row in rows:
            row.revoked_ts = _now()
            session.add(row)
            _session_cache.pop(row.token_hash, None)
        await session.commit()
        return len(rows)


async def revoke_session(user: str, session_id: int) -> bool:
    """End one of `user`'s sessions."""
    return await _revoke((UiSession.id == session_id) & (UiSession.user == user)) > 0


async def revoke_session_token(token: str) -> UiSession | None:
    """End the session behind a cookie value (sign-out); returns it if it was live."""
    row = await lookup_session(token, activity=False)
    if row is not None:
        await _revoke(UiSession.token_hash == _hash_session_token(token))
    return row


async def revoke_all_sessions(user: str) -> int:
    """End every session of `user` ("sign out everywhere", account disabled, password changed)."""
    _account_cache.pop(user.removeprefix(SESSION_PREFIX), None)
    return await _revoke(UiSession.user == user)
