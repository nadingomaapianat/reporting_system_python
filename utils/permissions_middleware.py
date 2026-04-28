"""
Runs after JWT middleware: enforces DCC page/actions on each request (reporting-node PermissionsGuard).
"""
from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from utils.dcc_permissions import (
    assert_user_has_permission,
    permission_error_response,
)

logger = logging.getLogger(__name__)

# No DCC check (still require JWT unless also public in JWT middleware)
EXEMPT_PATH_PREFIXES: tuple[str, ...] = (
    "/csrf/token",
    "/api/auth/validate-token",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/health",
)


class DccPermissionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method.upper() == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if any(path.startswith(p) for p in EXEMPT_PATH_PREFIXES):
            return await call_next(request)

        user = getattr(request.state, "user", None)
        if not user:
            # Public or unauthenticated path handled elsewhere
            return await call_next(request)

        try:
            assert_user_has_permission(user, path, request.method)
        except PermissionError as e:
            logger.info("Permission denied: %s %s — %s", request.method, path, e)
            return permission_error_response(str(e), 403)

        return await call_next(request)
