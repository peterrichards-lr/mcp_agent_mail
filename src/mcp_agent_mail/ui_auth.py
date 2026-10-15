"""Username + password sign-in for the web UI, with mandatory authenticator-app MFA.

For people without a GitHub account (GitHub sign-in lives in http.py). The
pieces are standard and use only the standard library:

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
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from sqlmodel import select

from .db import ensure_schema, get_session
from .models import UiUser

USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
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
    """A sign-in step failed; the message is safe to show the person signing in."""


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
    if not (code.isdigit() and len(code) == TOTP_DIGITS):
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
        return (await session.execute(select(UiUser).where(UiUser.username == username.lower()))).scalars().first()


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


async def create_user(username: str, password: str) -> UiUser:
    username = username.strip().lower()
    if not USERNAME_RE.match(username):
        raise ValueError("Username must be 1-64 characters: lowercase letters, digits, '-' or '_'.")
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
        user = (await session.execute(select(UiUser).where(UiUser.username == username.lower()))).scalars().first()
        if user is None:
            raise ValueError(f"No user named {username}.")
        for key, value in changes.items():
            setattr(user, key, value)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


async def set_password(username: str, password: str) -> UiUser:
    check_new_password(password)
    return await _update(username, password_hash=await asyncio.to_thread(hash_password, password), failed_attempts=0, locked_until=None)


async def reset_mfa(username: str) -> UiUser:
    """Forget the authenticator and recovery codes; the next sign-in enrols again."""
    return await _update(username, totp_secret=None, totp_pending_secret=None, totp_last_step=0, recovery_code_hashes=[])


async def set_disabled(username: str, disabled: bool) -> UiUser:
    return await _update(username, disabled=disabled, failed_attempts=0, locked_until=None)


# ---------------------------------------------------------------- sign-in steps


async def _record_failure(user_id: int) -> None:
    async with get_session() as session:
        user = await session.get(UiUser, user_id)
        if user is None:
            return
        user.failed_attempts += 1
        if user.failed_attempts >= MAX_FAILED_ATTEMPTS:
            user.locked_until = _now() + LOCKOUT
            user.failed_attempts = 0
        session.add(user)
        await session.commit()


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
        await _record_failure(user.id)
        raise AuthError("Wrong username or password.")
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
                await _record_failure(user_id)
                raise AuthError("That code didn't match.")
            user.recovery_code_hashes = [h for h in user.recovery_code_hashes if h != recovery]
        user.failed_attempts, user.last_login_ts = 0, _now()
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user
