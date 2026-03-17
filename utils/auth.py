from __future__ import annotations

import os
import sys
from urllib.parse import unquote
from jose import jwt, JWTError
from typing import Dict, Any, Optional
from fastapi import HTTPException, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


def _auth_log(msg: str) -> None:
    """Print to terminal (stdout) so auth debug appears in console, not in log file."""
    print(f"[AUTH] {msg}", flush=True, file=sys.stdout)

# JWT Configuration - must match Node (reporting-system-node) so same tokens work for both.
JWT_SECRET = os.getenv("JWT_SECRET", os.getenv("JWT_SECRET_KEY", "GRC_ADIB_2025"))
JWT_ALGORITHM = "HS256"

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
    Get JWT the same way as reporting-system-node (no frontend change needed).
    (1) Authorization: Bearer <token>
    (2) X-Forwarded-Authorization: Bearer <token> (when a proxy strips Authorization)
    (3) X-Export-Token: <token> (for server-to-server export proxy)
    (4) Cookie: reporting_node_token (set by Node with COOKIE_DOMAIN e.g. .adib.co.eg so browser sends it to Python)
    (5) Cookies: d_c_c_t_p_1 + d_c_c_t_p_2 (URL-decoded). Set by main backend (e.g. new_adib_backend /
        uat-backend.adib.co.eg). Token is split across two cookies due to size limits.
    (6) Query param: token or access_token (GET only) — fallback when cookies are not sent cross-origin.
    """
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()  # after "Bearer "
        if token:
            _auth_log(f"Token from: Authorization header (path={request.url.path})")
            return token

    # When Node (or another proxy) forwards the request, proxy may strip Authorization; accept these headers
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

    reporting_token = request.cookies.get("reporting_node_token")
    if reporting_token:
        _auth_log(f"Token from: cookie reporting_node_token (path={request.url.path})")
        return reporting_token

    part1 = request.cookies.get("d_c_c_t_p_1")
    part2 = request.cookies.get("d_c_c_t_p_2") or ""
    if part1:
        try:
            token = unquote(part1 + part2)
            if token:
                _auth_log(f"Token from: cookies d_c_c_t_p_* (path={request.url.path})")
                return token
        except Exception:
            pass

    # GET requests: allow token in query when cookies/header not sent (e.g. cross-origin export)
    if request.method.upper() == "GET":
        query_token = request.query_params.get("token") or request.query_params.get("access_token")
        if query_token and query_token.strip():
            _auth_log(f"Token from: query param token/access_token (path={request.url.path})")
            return query_token.strip()

    _auth_log(
        f"No token found for path={request.url.path} method={request.method} "
        "(checked: Authorization, X-Forwarded-Authorization, X-Export-Token, cookies reporting_node_token/d_c_c_t_p_*, query token/access_token)"
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
    Authorization header OR cookie (reporting_node_token or d_c_c_t_p_*).
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
                    "message": "Authorization token is missing (use Bearer header, cookies reporting_node_token / d_c_c_t_p_*, or for GET: ?token=)"
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

