from __future__ import annotations

import os
import re
import sys
from urllib.parse import unquote
from jose import jwt, JWTError
from typing import Dict, Any, Optional
from fastapi import HTTPException, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


def _cookie_from_header(cookie_header: Optional[str], name: str) -> Optional[str]:
    """Parse cookie value from raw Cookie header (same as Node)."""
    if not cookie_header or not cookie_header.strip():
        return None
    match = re.search(rf"(?:^|;\s*){re.escape(name)}=([^;]*)", cookie_header.strip())
    val = match.group(1).strip() if match else None
    return unquote(val) if val else None


def _token_from_split_cookies_in_header(cookie_header: Optional[str], prefix: str) -> Optional[str]:
    """Get JWT from split cookies (d_c_c_t_p_1/2 or iframe_d_c_c_t_p_1/2) from raw Cookie header."""
    if not cookie_header or not cookie_header.strip():
        return None
    parts = []
    i = 1
    while True:
        v = _cookie_from_header(cookie_header, f"{prefix}_{i}")
        if not v:
            break
        parts.append(v)
        i += 1
    if not parts:
        return None
    try:
        return unquote("".join(parts)) or None
    except Exception:
        return None


def _auth_log(msg: str) -> None:
    """Print to terminal (stdout) so auth debug appears in console, not in log file."""
    print(f"[AUTH] {msg}", flush=True, file=sys.stdout)

# JWT Configuration - must match Node (reporting-system-node) so same tokens work for both.
JWT_SECRET = os.getenv("JWT_SECRET", os.getenv("JWT_SECRET_KEY", "GRC_ADIB_2025"))
JWT_ALGORITHM = "HS256"

# Static cookie name for JWT (same as Node and frontend).
REPORTING_AUTH_COOKIE_NAME = "reporting_auth_token"

security = HTTPBearer(auto_error=False)

# Public paths: only these can be called WITHOUT JWT.
PUBLIC_PATHS = [
    "/csrf/token",
    "/api/auth/validate-token",
    "/api/auth/logout",
    "/docs",
    "/redoc",
    "/openapi.json",
]


def get_token_from_request(request: Request) -> Optional[str]:
    """
    Same order as Node: (1) Authorization headers, (2) Cookie, (3) GET query token.
    """
    # 1) Headers first — same as Node (Authorization, then X-Forwarded-Authorization, X-Export-Token)
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
        if token:
            _auth_log(f"Token from: Authorization header (path={request.url.path})")
            return token
    forwarded = request.headers.get("x-forwarded-authorization")
    if forwarded and forwarded.startswith("Bearer "):
        token = forwarded[7:].strip()
        if token:
            _auth_log(f"Token from: X-Forwarded-Authorization (path={request.url.path})")
            return token
    export_token = request.headers.get("x-export-token")
    if export_token and export_token.strip():
        _auth_log(f"Token from: X-Export-Token (path={request.url.path})")
        return export_token.strip()

    # 2) Cookie — same as Node (reporting_auth_token then split cookies)
    reporting_token = request.cookies.get(REPORTING_AUTH_COOKIE_NAME)
    if reporting_token:
        _auth_log(f"Token from: cookie {REPORTING_AUTH_COOKIE_NAME} (path={request.url.path})")
        return reporting_token

    for prefix in ("iframe_d_c_c_t_p", "d_c_c_t_p"):
        part1 = request.cookies.get(f"{prefix}_1")
        part2 = request.cookies.get(f"{prefix}_2") or ""
        if part1:
            try:
                token = unquote(part1 + part2)
                if token:
                    _auth_log(f"Token from: cookies {prefix}_* (path={request.url.path})")
                    return token
            except Exception:
                pass

    raw_cookie = request.headers.get("cookie")
    if raw_cookie:
        reporting_from_header = _cookie_from_header(raw_cookie, REPORTING_AUTH_COOKIE_NAME)
        if reporting_from_header:
            _auth_log(f"Token from: Cookie header {REPORTING_AUTH_COOKIE_NAME} (path={request.url.path})")
            return reporting_from_header
        for prefix in ("iframe_d_c_c_t_p", "d_c_c_t_p"):
            token = _token_from_split_cookies_in_header(raw_cookie, prefix)
            if token:
                _auth_log(f"Token from: Cookie header {prefix}_* (path={request.url.path})")
                return token

    # 3) GET: query param fallback
    if request.method.upper() == "GET":
        query_token = request.query_params.get("token") or request.query_params.get("access_token")
        if query_token and query_token.strip():
            _auth_log(f"Token from: query param token/access_token (path={request.url.path})")
            return query_token.strip()

    _auth_log(
        f"No token found for path={request.url.path} method={request.method} "
        f"(checked: Authorization, X-Forwarded-Authorization, X-Export-Token, cookies {REPORTING_AUTH_COOKIE_NAME}/d_c_c_t_p_*, query token/access_token)"
    )
    return None


def verify_token(token: str) -> Dict[str, Any]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError as e:
        _auth_log(f"Token verification failed: {e} (token prefix: {token[:20] if len(token) > 20 else token}...)")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Dict[str, Any]:
    """Dependency to get the current user from JWT token."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header is missing"
        )
    token = credentials.credentials
    payload = verify_token(token)
    return payload


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Validate JWT the same way as reporting-system-node: token from
    Authorization header OR cookie (REPORTING_AUTH_COOKIE_NAME or d_c_c_t_p_*).
    Frontend can call this API the same way it calls Node (same headers/cookies).
    """

    async def dispatch(self, request: Request, call_next):
        if request.method.upper() == "OPTIONS":
            return await call_next(request)

        if any(request.url.path.startswith(path) for path in PUBLIC_PATHS):
            return await call_next(request)

        token = get_token_from_request(request)
        if not token:
            _auth_log(f"401 path={request.url.path} method={request.method} -> reason: token missing")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "message": f"Authorization token is missing (use Bearer header, cookies {REPORTING_AUTH_COOKIE_NAME} / d_c_c_t_p_*, or for GET: ?token=)"
                },
            )

        try:
            payload = verify_token(token)
            request.state.user = payload
        except HTTPException as e:
            _auth_log(f"401 path={request.url.path} method={request.method} -> reason: token invalid or expired")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"success": False, "message": e.detail or "Invalid token"},
            )

        return await call_next(request)


def validate_token(token: str) -> Dict[str, Any]:
    """Validate a token and return the decoded payload - matching v2_backend format exactly."""
    if not token:
        return {"success": False, "message": "Token is required"}
    
    try:
        payload = verify_token(token)
        # Extract user info from token - matching v2_backend format exactly
        return {
            "success": True,
            "data": {
                "group": payload.get("group"),
                "title": payload.get("title"),
                "name": payload.get("name"),
                "id": payload.get("id"),
            }
        }
    except HTTPException as e:
        return {"success": False, "message": e.detail}
    except Exception as e:
        return {"success": False, "message": "Invalid or expired token"}

