from __future__ import annotations

import base64
import hashlib
import hmac
import re
import os
import secrets
from typing import Iterable, Sequence, Set

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from utils.auth import AUTH_COOKIE_PREFIXES, resolve_jwt_token_and_payload

CSRF_COOKIE_NAME = "csrfToken"
CSRF_HEADER_NAME = "x-csrf-token"
# Enforce CSRF on state‑changing methods only.
# OPTIONS, GET and HEAD are treated as safe so that reads and preflight can succeed.
DEFAULT_SAFE_METHODS: Set[str] = {"OPTIONS", "GET", "HEAD"}

# Except these GETs: a dashboard export writes a file and a saved-export row, so it is not a
# read. They need the token whenever the browser's login cookie is what authenticates them
# (server-to-server calls with a Bearer token carry no cookie a foreign page could ride on).
STATE_CHANGING_GET = re.compile(r"^/api/grc/[^/]+/export[-/]")


def _cookie_authenticated(request: Request) -> bool:
    # AUTH_COOKIE_PREFIXES comes from utils/auth.py: every cookie the JWT check reads, so the two
    # cannot drift (reporting_node_token was missing from a copy here, so its exports skipped this).
    return any(name.startswith(AUTH_COOKIE_PREFIXES) for name in request.cookies)


# Signed tokens: "<nonce>.<HMAC(key, nonce[|user id])>". The middleware used to accept any header
# that matched the cookie, so a self-chosen pair ("testcsrf"/"testcsrf") passed — pen test
# UB-2026-48, finding 4.1. Now only a token this server signed is accepted.
# Tokens checked here are only ever minted here (/csrf/token below; the front end and
# reporting_system_node both fetch them from this service), so the key is not shared with Node.
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\.[A-Za-z0-9_-]{43}")
_MIN_SECRET_LENGTH = 32
_csrf_key: bytes | None = None


def validate_csrf_secret() -> None:
    """
    Raise RuntimeError unless CSRF_SECRET is set (at least 32 characters). Called by main.py at
    startup, so a missing secret stops the service instead of failing every write later.
    No fallback: a key derived from JWT_SECRET let anyone who knows that secret mint CSRF tokens,
    and a random per-process key would refuse tokens issued by the other instances.
    """
    secret = (os.getenv("CSRF_SECRET") or "").strip()
    if len(secret) < _MIN_SECRET_LENGTH:
        raise RuntimeError(
            f"CSRF_SECRET is {'too short' if secret else 'not set'}: set CSRF_SECRET to a random value of at "
            f"least {_MIN_SECRET_LENGTH} characters (the same on every instance, different from JWT_SECRET)"
        )


def _key() -> bytes:
    """CSRF_SECRET (at least 32 characters). Every instance behind the load balancer must share it."""
    global _csrf_key
    if _csrf_key is None:
        validate_csrf_secret()
        _csrf_key = os.getenv("CSRF_SECRET").strip().encode("utf-8")
    return _csrf_key


def _sign(nonce: str, user_id: str = "") -> str:
    # The user id is signed in, not stored: a token issued to one user fails for another.
    message = f"{nonce}|{user_id}" if user_id else nonce
    digest = hmac.new(_key(), message.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def csrf_user_id(request: Request) -> str:
    """
    Id of the user whose valid JWT the request carries ("" for none), resolved the same way as
    the JWT check. Signature only: the revocation / session checks stay in JWTAuthMiddleware.
    """
    try:
        _, payload = resolve_jwt_token_and_payload(request)
    except Exception:
        return ""
    uid = (payload or {}).get("id") or (payload or {}).get("sub")
    return str(uid) if uid else ""


def create_csrf_token(user_id: str = "") -> str:
    """Generate a signed, cryptographically-strong CSRF token, bound to user_id when given."""
    nonce = secrets.token_hex(32)
    return f"{nonce}.{_sign(nonce, user_id)}"


def is_valid_csrf_token(token: str | None, user_id: str = "") -> bool:
    """True only for a well-formed token this server signed for this user (or for no user)."""
    # fullmatch: `$` in match() would also accept a trailing newline
    if not token or not _TOKEN_PATTERN.fullmatch(token):
        return False
    nonce, signature = token.split(".", 1)
    return hmac.compare_digest(signature, _sign(nonce, user_id))


class CSRFMiddleware(BaseHTTPMiddleware):
    """
    Double submit cookie CSRF protection.
    Requires clients to fetch /csrf/token, copy the token into the x-csrf-token header,
    and send credentials so the signed cookie is included.
    """

    def __init__(
        self,
        app,
        *,
        cookie_name: str = CSRF_COOKIE_NAME,
        header_name: str = CSRF_HEADER_NAME,
        safe_methods: Iterable[str] | None = None,
        exempt_paths: Sequence[str] | None = None,
    ):
        super().__init__(app)
        self.cookie_name = cookie_name
        self.header_name = header_name.lower()
        self.safe_methods = {method.upper() for method in (safe_methods or DEFAULT_SAFE_METHODS)}
        self.exempt_paths = frozenset(exempt_paths or ())

    async def dispatch(self, request: Request, call_next):
        method = request.method.upper()
        if method in self.safe_methods and not (
            method in ("GET", "HEAD")
            and STATE_CHANGING_GET.match(request.url.path)
            and _cookie_authenticated(request)
        ):
            return await call_next(request)

        # Exact paths only (a trailing slash is ignored). This used to be a prefix match, which
        # also exempted e.g. /api/auth/logout-anything or /docs-anything.
        path = request.url.path
        if (path.rstrip("/") or "/") in self.exempt_paths:
            return await call_next(request)

        csrf_cookie = request.cookies.get(self.cookie_name)
        csrf_header = request.headers.get(self.header_name)

        if (
            not csrf_cookie
            or not csrf_header
            # bytes: compare_digest raises on non-ASCII str, which turned a junk header into a 500
            or not hmac.compare_digest(csrf_cookie.encode("utf-8"), csrf_header.encode("utf-8"))
            # A logged-in request needs a token issued to that same user; an unbound one (fetched
            # without the login cookie) or another user's is refused. The front end refetches on 403.
            or not is_valid_csrf_token(csrf_cookie, csrf_user_id(request))
        ):
            return JSONResponse(status_code=403, content={"detail": "Invalid CSRF token"})

        return await call_next(request)


def set_csrf_cookie(response: Response, token: str, *, secure: bool) -> None:
    # Same-site subdomains (*.adib.co.eg) usually work with "lax"; use CSRF_COOKIE_SAMESITE=none + secure for stricter cross-site.
    samesite_raw = (os.getenv("CSRF_COOKIE_SAMESITE") or "lax").strip().lower()
    if samesite_raw not in ("strict", "lax", "none"):
        samesite_raw = "lax"
    if samesite_raw == "none" and not secure:
        samesite_raw = "lax"
    response.set_cookie(
        CSRF_COOKIE_NAME,
        token,
        httponly=True,
        secure=secure,
        samesite=samesite_raw,
        path="/",
    )

