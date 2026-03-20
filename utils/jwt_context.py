"""
Per-request JWT payload (async-safe via contextvars).

Set by JWTAuthMiddleware after successful verification so services can read
role / isAdmin / groupName without threading Request through every method.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Dict, Optional

_jwt_claims: ContextVar[Optional[Dict[str, Any]]] = ContextVar("jwt_claims", default=None)


def set_request_jwt_claims(claims: Optional[Dict[str, Any]]) -> None:
    """Store decoded JWT claims for the current request task."""
    _jwt_claims.set(claims)


def get_request_jwt_claims() -> Dict[str, Any]:
    """Return decoded JWT claims for the current request, or {}."""
    return _jwt_claims.get() or {}
