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

CSRF_COOKIE_NAME = "csrfToken"
CSRF_HEADER_NAME = "x-csrf-token"
# Enforce CSRF on state‑changing methods only.
# OPTIONS, GET and HEAD are treated as safe so that reads and preflight can succeed.
DEFAULT_SAFE_METHODS: Set[str] = {"OPTIONS", "GET", "HEAD"}


# Signed tokens: "<nonce>.<HMAC(key, nonce)>", the same scheme as adib_backend and
# reporting_system_node (src/csrf/csrf-token.ts). The middleware used to accept any header that
# matched the cookie, so a self-chosen pair ("testcsrf"/"testcsrf") passed — pen test UB-2026-48,
# finding 4.1. Now only a token this server signed is accepted.
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\.[A-Za-z0-9_-]{43}")
_MIN_SECRET_LENGTH = 32
_csrf_key: bytes | None = None


def _key() -> bytes:
    """
    CSRF_SECRET (at least 32 characters), else a key derived from JWT_SECRET / JWT_SECRET_KEY
    (domain-separated). Every instance behind the load balancer must share it. No random
    fallback: a per-process key would refuse tokens issued by the other instances.
    """
    global _csrf_key
    if _csrf_key is None:
        explicit = (os.getenv("CSRF_SECRET") or "").strip()
        jwt_secret = (os.getenv("JWT_SECRET") or os.getenv("JWT_SECRET_KEY") or "").strip()
        if explicit:
            if len(explicit) < _MIN_SECRET_LENGTH:
                raise RuntimeError(f"CSRF_SECRET must be at least {_MIN_SECRET_LENGTH} characters")
            _csrf_key = explicit.encode("utf-8")
        elif jwt_secret:
            _csrf_key = hmac.new(jwt_secret.encode("utf-8"), b"csrf-token-v1", hashlib.sha256).digest()
        else:
            raise RuntimeError("CSRF_SECRET or JWT_SECRET (or JWT_SECRET_KEY) must be set")
    return _csrf_key


def _sign(nonce: str) -> str:
    digest = hmac.new(_key(), nonce.encode("ascii"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def create_csrf_token() -> str:
    """Generate a signed, cryptographically-strong CSRF token."""
    nonce = secrets.token_hex(32)
    return f"{nonce}.{_sign(nonce)}"


def is_valid_csrf_token(token: str | None) -> bool:
    """True only for a well-formed token whose signature this server produced."""
    # fullmatch: `$` in match() would also accept a trailing newline
    if not token or not _TOKEN_PATTERN.fullmatch(token):
        return False
    nonce, signature = token.split(".", 1)
    return hmac.compare_digest(signature, _sign(nonce))


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
        if request.method.upper() in self.safe_methods:
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
            or not is_valid_csrf_token(csrf_cookie)
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

