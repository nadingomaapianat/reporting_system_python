from __future__ import annotations

from jose import jwt, JWTError
from typing import Dict, Any, Optional
from fastapi import HTTPException, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# JWT Configuration - matching v2_backend
# Tokens are generated in v2_backend, we only validate them here
JWT_SECRET = "GRC_ADIB_2025"
JWT_ALGORITHM = "HS256"

security = HTTPBearer(auto_error=False)

# Public paths that don't require authentication
PUBLIC_PATHS = [
    "/csrf/token",
    "/api/auth/validate-token",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/exports",
    # Reporting exports & dynamic/dynamic-dashboard/execute-sql endpoints are read-only or proxied via secured backends.
    # Allow them without JWT so the reporting frontend can load dashboards/reports in dev/embedded mode.
    "/api/exports",
    "/api/reports/dynamic",
    "/api/reports/dynamic-dashboard",
    "/api/reports/execute-sql",
    # Bank check reports (auth handled upstream via cookies)
    "/api/reports/bank-check",
    "/api/reports/enhanced-bank-check",
]


def verify_token(token: str) -> Dict[str, Any]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
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
    """Middleware to validate JWT tokens on all requests except public paths."""
    
    async def dispatch(self, request: Request, call_next):
        # Always allow CORS preflight through (OPTIONS) so CORSMiddleware can respond
        if request.method.upper() == "OPTIONS":
            return await call_next(request)
        
        # Skip authentication for public paths
        if any(request.url.path.startswith(path) for path in PUBLIC_PATHS):
            return await call_next(request)
        
        # Check for Authorization header
        auth_header = request.headers.get("authorization")
        if not auth_header:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"success": False, "message": "Authorization header is missing"}
            )
        
        # Extract token
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"success": False, "message": "Bearer token is missing"}
            )
        
        token = auth_header.split("Bearer ")[1]
        
        try:
            payload = verify_token(token)
            # Attach user to request
            request.state.user = payload
        except HTTPException as e:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"success": False, "message": e.detail}
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

