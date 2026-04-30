from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import unquote

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt, JWTError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from utils.jwt_context import set_request_jwt_claims


# =============================================================================
# Config
# =============================================================================
JWT_SECRET = os.getenv("JWT_SECRET", os.getenv("JWT_SECRET_KEY", "GRC_ADIB_2025"))
JWT_ALGORITHM = "HS256"

# Cookie names — must match reporting_system_node2 / new_adib_backend.
REPORTING_AUTH_COOKIE_NAME = "reporting_auth_token"  # legacy single cookie
REPORTING_NODE_TOKEN_LEGACY = "reporting_node_token"  # current single cookie
REPORTING_NODE_TOKEN_PREFIX = "reporting_node_token"  # split chunks: _1, _2, …
IFRAME_COOKIE_PREFIX = "iframe_d_c_c_t_p"
MAIN_COOKIE_PREFIX = "d_c_c_t_p"

# Maximum number of split-cookie parts to attempt to reconstruct (matches Node).
_MAX_SPLIT_PARTS = 64

# Set REPORTING_ALLOW_QUERY_TOKEN_AUTH=false in production to disable ?token= / ?access_token=
# (avoids leaks in logs).
_ALLOW_QUERY_TOKEN_GET = os.getenv("REPORTING_ALLOW_QUERY_TOKEN_AUTH", "true").lower() in (
    "true", "1", "yes",
)

# Public paths: allowed without a JWT. Extended for debug below.
PUBLIC_PATHS: List[str] = [
    "/csrf/token",
    "/api/auth/validate-token",
    "/api/auth/logout",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/health",
]
if os.getenv("REPORTING_DEBUG_COOKIES_ENDPOINT", "").lower() in ("1", "true", "yes"):
    PUBLIC_PATHS.append("/debug/cookies")

# HTTPBearer dependency for `get_current_user` (auto_error=False so we can return
# a uniform 401 from middleware/dependency without FastAPI's default plain-text 403).
security = HTTPBearer(auto_error=False)

_LOG_AUTH_HEADERS = os.getenv("REPORTING_LOG_AUTH_HEADERS", "").lower() in ("1", "true", "yes")
_auth_header_logger = logging.getLogger("reporting.auth.headers")


# =============================================================================
# Logging helpers
# =============================================================================
def _auth_log(msg: str) -> None:
    """Print to stdout so auth debug appears in console (not log file)."""
    print(f"[AUTH] {msg}", flush=True, file=sys.stdout)


def log_incoming_auth_headers(request: Request) -> None:
    """
    Log Cookie / Authorization presence (lengths only — never full secrets) when
    REPORTING_LOG_AUTH_HEADERS=true.
    """
    if not _LOG_AUTH_HEADERS:
        return
    cookie_header = request.headers.get("cookie")
    auth_h = request.headers.get("authorization")
    fwd = request.headers.get("x-forwarded-authorization")
    export_t = request.headers.get("x-export-token")
    _auth_header_logger.info(
        "incoming_headers path=%s method=%s cookie_header_len=%s has_authorization=%s "
        "has_x_forwarded_authorization=%s has_x_export_token=%s cookie_names=%s",
        request.url.path,
        request.method,
        len(cookie_header) if cookie_header else 0,
        bool(auth_h),
        bool(fwd),
        bool(export_t and export_t.strip()),
        list(request.cookies.keys()),
    )


# =============================================================================
# Cookie parsing — same priority as reporting_system_node2 `extract-token.ts`
# =============================================================================
def _decode_cookie_value(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    try:
        return unquote(v)
    except Exception:
        return v


def _cookie_from_header(cookie_header: Optional[str], name: str) -> Optional[str]:
    """Parse cookie value from raw Cookie header (fallback when request.cookies is empty)."""
    if not cookie_header or not cookie_header.strip():
        return None
    match = re.search(rf"(?:^|;\s*){re.escape(name)}=([^;]*)", cookie_header.strip())
    val = match.group(1).strip() if match else None
    return unquote(val) if val else None


def _read_split_cookies(cookies: Mapping[str, str], prefix: str) -> Optional[str]:
    """
    Reconstruct a chunked token: prefix_1 + prefix_2 + … then URL-decode the join.
    Mirrors reporting_node `readSplitCookies` exactly.
    """
    parts: List[str] = []
    for i in range(1, _MAX_SPLIT_PARTS + 1):
        raw = cookies.get(f"{prefix}_{i}")
        if raw is None or str(raw).strip() == "":
            break
        parts.append(str(raw))
    if not parts:
        return None
    joined = "".join(parts)
    try:
        return unquote(joined)
    except Exception:
        return joined


def _split_cookies_from_header(cookie_header: Optional[str], prefix: str) -> Optional[str]:
    if not cookie_header or not cookie_header.strip():
        return None
    parts: List[str] = []
    for i in range(1, _MAX_SPLIT_PARTS + 1):
        v = _cookie_from_header(cookie_header, f"{prefix}_{i}")
        if not v:
            break
        parts.append(v)
    if not parts:
        return None
    try:
        return unquote("".join(parts)) or None
    except Exception:
        return None


def _reporting_node_token_from_cookies(cookies: Mapping[str, str]) -> Optional[str]:
    """Single `reporting_node_token` first, then split `reporting_node_token_1`+…"""
    single = cookies.get(REPORTING_NODE_TOKEN_LEGACY)
    if single and str(single).strip():
        return _decode_cookie_value(str(single))
    return _read_split_cookies(cookies, REPORTING_NODE_TOKEN_PREFIX)


def candidate_jwt_strings(request: Request) -> List[str]:
    """
    Candidate JWT strings in the same priority order as reporting_node `getCandidateTokens`:
      1. reporting_node_token (single or split)
      2. legacy reporting_auth_token
      3. Authorization: Bearer (and X-Forwarded-Authorization)
      4. X-Export-Token
      5. iframe_d_c_c_t_p_* split cookies
      6. d_c_c_t_p_* split cookies (set by main app on shared parent domain)
      7. Fallback: parse raw `Cookie` header (some proxies strip request.cookies)
      8. Optional: ?token= / ?access_token= for GET requests when feature flag enabled
    """
    cookies: Mapping[str, str] = request.cookies or {}
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

    # 1. reporting_node_token (single or split)
    add(_reporting_node_token_from_cookies(cookies))

    # 2. legacy single cookie
    legacy = cookies.get(REPORTING_AUTH_COOKIE_NAME)
    if legacy:
        add(_decode_cookie_value(str(legacy)))

    # 3. Authorization headers
    auth_header = request.headers.get("authorization") or request.headers.get("Authorization") or ""
    if auth_header.startswith("Bearer "):
        add(auth_header.split("Bearer ", 1)[1].strip())
    fwd = request.headers.get("x-forwarded-authorization") or ""
    if fwd.startswith("Bearer "):
        add(fwd.split("Bearer ", 1)[1].strip())

    # 4. Explicit export header
    export_token = request.headers.get("x-export-token")
    if export_token and export_token.strip():
        add(export_token.strip())

    # 5. iframe split cookies
    add(_read_split_cookies(cookies, IFRAME_COOKIE_PREFIX))

    # 6. main-app split cookies (set by DCC on .pianat.ai / .adib.co.eg)
    add(_read_split_cookies(cookies, MAIN_COOKIE_PREFIX))

    # 7. Fallback: parse raw Cookie header (some reverse proxies don't populate request.cookies)
    raw_cookie = request.headers.get("cookie")
    if raw_cookie:
        add(_cookie_from_header(raw_cookie, REPORTING_NODE_TOKEN_LEGACY))
        add(_split_cookies_from_header(raw_cookie, REPORTING_NODE_TOKEN_PREFIX))
        add(_cookie_from_header(raw_cookie, REPORTING_AUTH_COOKIE_NAME))
        add(_split_cookies_from_header(raw_cookie, IFRAME_COOKIE_PREFIX))
        add(_split_cookies_from_header(raw_cookie, MAIN_COOKIE_PREFIX))

    # 8. Optional query param fallback (GET only) — disable in prod via env.
    if _ALLOW_QUERY_TOKEN_GET and request.method.upper() == "GET":
        qp = request.query_params
        for key in ("token", "access_token"):
            v = qp.get(key)
            if v and v.strip():
                add(v.strip())

    return out


# =============================================================================
# JWT verify / decode
# =============================================================================
def _decode_jwt_payload(token: str) -> Optional[Dict[str, Any]]:
    """Decode without raising — returns None on any error."""
    if not token or not token.strip():
        return None
    try:
        return jwt.decode(token.strip(), JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None


def verify_token(token: str) -> Dict[str, Any]:
    """Verify and decode a JWT token (raises 401 on failure)."""
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError as e:
        _auth_log(
            f"Token verification failed: {e} "
            f"(token prefix: {token[:20] if len(token) > 20 else token}...)"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


def resolve_jwt_token_and_payload(
    request: Request,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """First candidate that verifies → (token_string, payload); otherwise (None, None)."""
    for raw in candidate_jwt_strings(request):
        payload = _decode_jwt_payload(raw)
        if payload:
            return raw, payload
    return None, None


# =============================================================================
# FastAPI dependency
# =============================================================================
async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Dict[str, Any]:
    """
    Resolve the current user from any supported token source (Authorization or cookies).
    Prefer Authorization for backwards compatibility, then fall back to cookies.
    """
    if credentials and credentials.credentials:
        return verify_token(credentials.credentials)

    # No Authorization header → try cookies / other candidates so endpoints that depend on this
    # also work when called via cookies (matches Node behavior).
    _, payload = resolve_jwt_token_and_payload(request)
    if payload:
        return payload

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authorization token is missing",
    )


# =============================================================================
# Middleware
# =============================================================================
class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Validate JWT the same way as reporting_system_node2: token from
    Authorization header OR cookie (reporting_node_token / iframe_d_c_c_t_p_* / d_c_c_t_p_*).
    Frontend can call this API the same way it calls Node (same headers/cookies).
    Also enforces the shared `blocked_tokens` revocation list.
    """

    async def dispatch(self, request: Request, call_next):
        # Clear per-request JWT context unless we authenticate below
        set_request_jwt_claims(None)

        if request.method.upper() == "OPTIONS":
            return await call_next(request)

        log_incoming_auth_headers(request)

        if any(request.url.path.startswith(path) for path in PUBLIC_PATHS):
            return await call_next(request)

        token_str, payload = resolve_jwt_token_and_payload(request)
        if not payload or not token_str:
            _auth_log(
                f"401 path={request.url.path} method={request.method} -> reason: "
                f"no_valid_jwt cookie_names={list((request.cookies or {}).keys())}"
            )
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "message": (
                        "Authorization token is missing (use Bearer header, "
                        f"cookies {REPORTING_NODE_TOKEN_LEGACY} / "
                        f"{IFRAME_COOKIE_PREFIX}_* / {MAIN_COOKIE_PREFIX}_*"
                        + (", or for GET: ?token=" if _ALLOW_QUERY_TOKEN_GET else "")
                        + ")"
                    ),
                },
            )

        # Reject revoked tokens (DCC inserts JWTs into `blocked_tokens` on logout / force-logout).
        try:
            from utils.token_blocklist import is_token_blocked

            blocked = await asyncio.to_thread(is_token_blocked, token_str)
            if blocked:
                _auth_log(
                    f"401 path={request.url.path} method={request.method} -> reason: "
                    f"token_revoked user={payload.get('id') or payload.get('sub') or '?'}"
                )
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={
                        "success": False,
                        "message": "Session has been revoked. Please sign in again.",
                    },
                )
        except Exception:
            # Fail-open on unexpected errors so DB blip does not lock everyone out;
            # `is_token_blocked` already swallows DB errors with a warning.
            pass

        request.state.user = payload
        set_request_jwt_claims(payload)
        return await call_next(request)


# =============================================================================
# Used by /api/auth/validate-token endpoint
# =============================================================================
def validate_token(token: str) -> Dict[str, Any]:
    """Validate a token and return the decoded payload — matches v2_backend response shape."""
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
    except Exception:
        return {"success": False, "message": "Invalid or expired token"}
