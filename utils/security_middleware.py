"""
Bank-grade security middleware: headers, trust proxy, rate limiting.
Aligns with new_adib_backend and reporting_system_node2.
"""
import ipaddress
import os
import time
import logging
from typing import Optional
from utils.error_sanitizer import is_production
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

logger = logging.getLogger(__name__)

# --- Trust proxy: use X-Forwarded-For when behind reverse proxy ---
TRUST_PROXY = os.getenv("TRUST_PROXY", "1").strip().lower() in ("1", "true", "yes")
# Number of reverse proxies in front of the app that append to X-Forwarded-For (nginx = 1).
try:
    TRUSTED_PROXY_HOPS = max(1, int(os.getenv("TRUSTED_PROXY_HOPS", "1")))
except ValueError:
    TRUSTED_PROXY_HOPS = 1


def _is_proxy_peer(host: str) -> bool:
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return addr.is_private or addr.is_loopback


def get_client_ip(request: Request) -> str:
    """Real client IP when behind proxy (R-WAPT / rate limit).

    The left-most X-Forwarded-For entries are whatever the client sent, so rotating them used to
    dodge the limit. Only a request whose direct peer is a proxy (private/loopback address) is
    trusted, and the IP is the entry the trusted proxies appended, counted from the right.
    """
    peer = request.client.host if request.client else "0.0.0.0"
    if not TRUST_PROXY or not _is_proxy_peer(peer):
        return peer
    forwarded = request.headers.get("x-forwarded-for")
    if not forwarded:
        return peer
    hops = [part.strip() for part in forwarded.split(",") if part.strip()]
    if not hops:
        return peer
    return hops[max(0, len(hops) - TRUSTED_PROXY_HOPS)]


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


# --- Rate limiting per IP (same as Node): RATE_LIMIT_MAX requests per RATE_LIMIT_WINDOW_SECONDS ---
# /csrf/token exempt so frontend/proxy can fetch CSRF without 429 under load
RATE_LIMIT_WINDOW = _int_env("RATE_LIMIT_WINDOW_SECONDS", 60)
RATE_LIMIT_MAX = _int_env("RATE_LIMIT_MAX", 40)
RATE_LIMIT_EXEMPT_PATHS = ("/csrf/token",)
_store: dict[str, tuple[int, float]] = {}
_store_ttl: float = 0


def _clean_store():
    global _store_ttl
    now = time.time()
    if now - _store_ttl > 60:
        _store_ttl = now
        to_del = [k for k, (_, start) in _store.items() if now - start > RATE_LIMIT_WINDOW]
        for k in to_del:
            del _store[k]


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method == "OPTIONS":
            return await call_next(request)
        path = request.url.path.rstrip("/") or "/"
        if any(path == p or path.startswith(p + "/") for p in RATE_LIMIT_EXEMPT_PATHS):
            return await call_next(request)
        ip = get_client_ip(request)
        now = time.time()
        _clean_store()
        if ip in _store:
            count, start = _store[ip]
            if now - start >= RATE_LIMIT_WINDOW:
                _store[ip] = (1, now)
            else:
                count += 1
                if count > RATE_LIMIT_MAX:
                    return JSONResponse(
                        status_code=429,
                        content={"detail": "Too many requests. Please try again later."},
                    )
                _store[ip] = (count, start)
        else:
            _store[ip] = (1, now)
        return await call_next(request)


# --- Uploads (WAPT06 / R-WAPT02) ---
# Per-user quota on file upload (multipart) requests, so spacing requests out (e.g. 10 s apart)
# cannot be used to upload without limit; and a request-size cap checked before the body is read
# (read_upload() caps each file at UPLOAD_MAX_FILE_MB once it has arrived).
UPLOAD_LIMIT_PER_HOUR = _int_env("UPLOAD_LIMIT_PER_HOUR", 50)
UPLOAD_LIMIT_PER_DAY = _int_env("UPLOAD_LIMIT_PER_DAY", 100)
UPLOAD_MAX_REQUEST_BYTES = _int_env("UPLOAD_MAX_REQUEST_MB", 100) * 1024 * 1024
_upload_counts: dict[str, dict[str, list]] = {}


def _count_upload(user_key: str) -> Optional[str]:
    """Count one upload for the user; return an error message when over the hourly/daily limit."""
    now = time.time()
    hour_start, day_start = int(now // 3600) * 3600, int(now // 86400) * 86400
    entry = _upload_counts.setdefault(user_key, {"hour": [hour_start, 0], "day": [day_start, 0]})
    if entry["hour"][0] != hour_start:
        entry["hour"] = [hour_start, 0]
    if entry["day"][0] != day_start:
        entry["day"] = [day_start, 0]
    if entry["day"][1] + 1 > UPLOAD_LIMIT_PER_DAY:
        return f"Upload limit exceeded: maximum {UPLOAD_LIMIT_PER_DAY} uploads per day. Try again tomorrow."
    if entry["hour"][1] + 1 > UPLOAD_LIMIT_PER_HOUR:
        return f"Upload limit exceeded: maximum {UPLOAD_LIMIT_PER_HOUR} uploads per hour. Try again later."
    entry["hour"][1] += 1
    entry["day"][1] += 1
    return None


class UploadLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        content_type = request.headers.get("content-type", "").lower()
        if request.method == "OPTIONS" or not content_type.startswith("multipart/form-data"):
            return await call_next(request)
        try:
            length = int(request.headers.get("content-length", "0"))
        except ValueError:
            length = 0
        if length > UPLOAD_MAX_REQUEST_BYTES:
            return JSONResponse(status_code=413, content={"detail": "File too large"})
        # Only a verified token identifies the user; without one the route's auth answers 401.
        from utils.auth import resolve_jwt_token_and_payload

        _, payload = resolve_jwt_token_and_payload(request)
        user_id = (payload or {}).get("id") or (payload or {}).get("sub")
        if user_id is not None:
            message = _count_upload(str(user_id))
            if message:
                return JSONResponse(status_code=429, content={"detail": message})
        return await call_next(request)


# HSTS in production (same header as the Node apps); browsers ignore it on plain http.
_HSTS = is_production()


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Set security headers (R-WAPT03); remove X-Powered-By."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        if hasattr(response, "headers"):
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "SAMEORIGIN"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
                "img-src 'self' data: https:; font-src 'self' data:; connect-src 'self'; "
                "frame-ancestors 'self'; base-uri 'self'; object-src 'none'"
            )
            if _HSTS:
                response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
            if "x-powered-by" in response.headers:
                del response.headers["x-powered-by"]
        return response
