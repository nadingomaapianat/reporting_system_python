from __future__ import annotations

import hmac
import secrets
from typing import Iterable, Sequence, Set

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

CSRF_COOKIE_NAME = "csrfToken"
CSRF_HEADER_NAME = "x-csrf-token"
# We still enforce CSRF on all application methods (GET/POST/PUT/DELETE/PATCH),
# but we explicitly treat OPTIONS as safe so that CORS preflight can succeed.
DEFAULT_SAFE_METHODS: Set[str] = {"OPTIONS"}


def create_csrf_token() -> str:
    """Generate a cryptographically-strong CSRF token."""
    return secrets.token_hex(32)


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
        self.exempt_paths = tuple(exempt_paths or ())

    async def dispatch(self, request: Request, call_next):
        if request.method.upper() in self.safe_methods:
            return await call_next(request)

        path = request.url.path
        if any(path.startswith(prefix) for prefix in self.exempt_paths):
            return await call_next(request)

        csrf_cookie = request.cookies.get(self.cookie_name)
        csrf_header = request.headers.get(self.header_name)

        if not csrf_cookie or not csrf_header or not hmac.compare_digest(csrf_cookie, csrf_header):
            return JSONResponse(status_code=403, content={"detail": "Invalid CSRF token"})

        return await call_next(request)


def set_csrf_cookie(response: Response, token: str, *, secure: bool) -> None:
    response.set_cookie(
        CSRF_COOKIE_NAME,
        token,
        httponly=True,
        secure=secure,
        samesite="strict",
        path="/",
    )

