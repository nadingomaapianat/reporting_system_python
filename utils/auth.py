from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import unquote

from jose import jwt, JWTError
from fastapi import HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# JWT Configuration — same env keys as reporting-node (`src/auth/jwt-secret.ts`)
def _jwt_secret() -> str:
    s = (os.getenv("JWT_SECRET") or os.getenv("JWT_SECRET_KEY") or "").strip()
    if s:
        return s
    if os.getenv("NODE_ENV", "").lower() == "production" or os.getenv("ENVIRONMENT", "").lower() == "production":
        raise RuntimeError("JWT_SECRET or JWT_SECRET_KEY must be set in production")
    return "GRC_ADIB_2025"


JWT_ALGORITHM = "HS256"

# Public paths that don't require authentication (CSRF bootstrap + token validation only).
PUBLIC_PATHS = [
    "/csrf/token",
    "/api/auth/validate-token",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/health",
]


def _decode_cookie_value(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    try:
        return unquote(v)
    except Exception:
        return v


def _read_split_cookies(cookies: Mapping[str, str], prefix: str) -> Optional[str]:
    """Same as reporting_node `readSplitCookies`: prefix_1 + prefix_2 + … then decodeURIComponent."""
    parts: List[str] = []
    i = 1
    while i <= 64:
        key = f"{prefix}_{i}"
        raw = cookies.get(key)
        if raw is None or str(raw).strip() == "":
            break
        parts.append(str(raw))
        i += 1
    if not parts:
        return None
    joined = "".join(parts)
    try:
        return unquote(joined)
    except Exception:
        return joined


def _reporting_node_token_from_cookies(cookies: Mapping[str, str]) -> Optional[str]:
    """Single `reporting_node_token` or split `reporting_node_token_1`… (matches `extract-token.ts`)."""
    single = cookies.get("reporting_node_token")
    if single and str(single).strip():
        return _decode_cookie_value(str(single))
    return _read_split_cookies(cookies, "reporting_node_token")


def candidate_jwt_strings(request: Request) -> List[str]:
    """
    Candidate JWT strings in the same priority order as reporting_node `getCandidateTokens`:
      1. reporting_node_token (single or split)
      2. Authorization: Bearer (if different from #1)
      3. iframe_d_c_c_t_p_* split cookies
      4. d_c_c_t_p_* split cookies
    """
    cookies: Mapping[str, str] = request.cookies
    out: List[str] = []
    seen: set[str] = set()

    def add(t: Optional[str]) -> None:
        if not t:
            return
        s = t.strip()
        if not s or s in seen:
            return
        seen.add(s)
        out.append(s)

    reporting = _reporting_node_token_from_cookies(cookies)
    if reporting:
        add(reporting)

    auth_header = request.headers.get("authorization") or request.headers.get("Authorization") or ""
    if auth_header.startswith("Bearer "):
        bearer = auth_header.split("Bearer ", 1)[1].strip()
        if bearer and bearer != reporting:
            add(bearer)

    iframe = _read_split_cookies(cookies, "iframe_d_c_c_t_p")
    if iframe:
        add(iframe)

    main = _read_split_cookies(cookies, "d_c_c_t_p")
    if main:
        add(main)

    return out


def _decode_jwt_payload(token: str) -> Optional[Dict[str, Any]]:
    try:
        return jwt.decode(token.strip(), _jwt_secret(), algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None


def resolve_jwt_payload(request: Request) -> Optional[Dict[str, Any]]:
    """First candidate that verifies with shared JWT secret."""
    for raw in candidate_jwt_strings(request):
        payload = _decode_jwt_payload(raw)
        if payload:
            return payload
    return None


def resolve_jwt_token_and_payload(
    request: Request,
) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Return the first verifying JWT *string* together with its decoded payload."""
    for raw in candidate_jwt_strings(request):
        payload = _decode_jwt_payload(raw)
        if payload:
            return raw, payload
    return None, None


def verify_token(token: str) -> Dict[str, Any]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, _jwt_secret(), algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )


async def get_current_user(request: Request) -> Dict[str, Any]:
    """User set by JWTAuthMiddleware on `request.state.user`."""
    user = getattr(request.state, "user", None)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    return user


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Validate JWT from cookies (reporting_node / iframe / main app) or Authorization: Bearer — same sources as reporting_node."""

    async def dispatch(self, request: Request, call_next):
        if request.method.upper() == "OPTIONS":
            return await call_next(request)

        if any(request.url.path.startswith(path) for path in PUBLIC_PATHS):
            return await call_next(request)

        token_str, payload = resolve_jwt_token_and_payload(request)
        if not payload:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "message": "Missing or invalid session (no valid JWT in cookies or Authorization)",
                },
            )

        # Reject revoked tokens (DCC adds JWTs to `blocked_tokens` on logout / force-logout).
        try:
            from utils.token_blocklist import is_token_blocked

            blocked = await asyncio.to_thread(is_token_blocked, token_str)
            if blocked:
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={
                        "success": False,
                        "message": "Session has been revoked. Please sign in again.",
                    },
                )
        except Exception:
            # Fail-open on unexpected errors so DB blip does not lock everyone out;
            # the inner function already swallows DB errors with a warning.
            pass

        try:
            from utils.db_permissions import merge_permissions_into_user

            request.state.user = await asyncio.to_thread(
                merge_permissions_into_user, dict(payload)
            )
        except HTTPException as e:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"success": False, "message": e.detail},
            )

        return await call_next(request)


def validate_token(token: str) -> Dict[str, Any]:
    """Validate a token and return the decoded payload - matching v2_backend format exactly."""
    if not token:
        return {"success": False, "message": "Token is required"}

    try:
        payload = verify_token(token)
        return {
            "success": True,
            "data": {
                "group": payload.get("group"),
                "title": payload.get("title"),
                "name": payload.get("name"),
                "id": payload.get("id"),
            },
        }
    except HTTPException as e:
        return {"success": False, "message": e.detail}
    except Exception as e:
        return {"success": False, "message": "Invalid or expired token"}
