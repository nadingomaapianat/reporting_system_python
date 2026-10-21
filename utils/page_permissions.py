"""
Page-permission gate for every Python reporting route.

Each request is matched against RULES (first match wins). A rule names the DCC page
rows the caller's group must hold — the same rows the main backend's group service
edits in the permission matrix (new_adib_backend `src/group/page-catalog.ts` names):

    any_of=[ [("Dashboard", SHOW), ("Risk Catalog", SHOW)], ... ]

`any_of` is a list of alternatives; each alternative is a list of (page, actions) that
must ALL hold; an action list passes when any one of its flags is true. A path that no
rule matches is refused, so a new endpoint cannot ship unguarded by accident.

Permissions are read live from Users -> Groups.permissions (cached briefly), not from
the token: the main-app token carries no permissions claim by design.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from utils.auth import PUBLIC_PATHS, resolve_jwt_token_and_payload
from utils.reporting_access import is_reporting_admin

logger = logging.getLogger(__name__)

SHOW = ("show",)
CREATE_OR_EDIT = ("create", "edit")
EDIT_OR_DELETE = ("edit", "delete")
DELETE = ("delete",)
CREATE = ("create",)

Requirement = List[Tuple[str, Sequence[str]]]

LOGIN_ONLY = "login"
SUPER_ADMIN = "super_admin"


def _dashboard_module(module_page: str) -> List[Requirement]:
    return [[("Dashboard", SHOW), (module_page, SHOW)]]


def _reporting_tab(actions: Sequence[str]) -> List[Requirement]:
    # /reports and /transaction-report render the same ReportsPage, and the header shows the
    # Reports tab for "Reports" or the legacy "Reporting" row — all three open these endpoints.
    return [[("Reports", actions)], [("Reporting", actions)], [("Transactions", actions)]]


DASHBOARD_OR_REPORTS_SHOW: List[Requirement] = [[("Dashboard", SHOW)]] + _reporting_tab(SHOW)

# (methods or None for any, path regex, LOGIN_ONLY | SUPER_ADMIN | list of alternatives)
RULES: List[Tuple[Optional[Tuple[str, ...]], "re.Pattern[str]", Any]] = [
    (None, re.compile(r"^/api/auth/"), LOGIN_ONLY),
    (("GET",), re.compile(r"^/$"), LOGIN_ONLY),
    # Developer diagnostics
    (None, re.compile(r"^/debug/"), SUPER_ADMIN),
    # Dashboard module exports: the dashboard tab AND the module's own page
    (None, re.compile(r"^/api/grc/controls/"), _dashboard_module("Control Catalog")),
    (None, re.compile(r"^/api/grc/risks/"), _dashboard_module("Risk Catalog")),
    (None, re.compile(r"^/api/grc/kris/"), _dashboard_module("KRI Catalog")),
    (None, re.compile(r"^/api/grc/incidents/"), _dashboard_module("Incidents Catalog")),
    (None, re.compile(r"^/api/grc/comply/"), _dashboard_module("cs Dashboard")),
    # Dynamic charts (Dashboard) — authoring needs create/edit
    (("POST",), re.compile(r"^/api/reports/dynamic-dashboard/save-chart$"), [[("Dashboard", CREATE_OR_EDIT)]]),
    (("DELETE",), re.compile(r"^/api/reports/dynamic-dashboard/charts/[^/]+$"), [[("Dashboard", EDIT_OR_DELETE)]]),
    (("GET",), re.compile(r"^/api/reports/dynamic-dashboard/charts$"), [[("Dashboard", SHOW)]]),
    # ICR template parser, called by reporting Node /api/icr/admin/parse-template (same page check)
    (("POST",), re.compile(r"^/parse-template$"), [[("ICR Templates", CREATE_OR_EDIT)]]),
    # Report builder: used by dynamic charts (Dashboard) and ReportGenerator (Reports/Transactions)
    (None, re.compile(r"^/api/reports/dynamic(/preview)?$"), DASHBOARD_OR_REPORTS_SHOW),
    # Reports / Transactions tab
    (("DELETE",), re.compile(r"^/api/exports/[^/]+$"), _reporting_tab(DELETE)),
    (("DELETE",), re.compile(r"^/api/scheduled-reports(/|$)"), _reporting_tab(DELETE)),
    (None, re.compile(r"^/api/reports/schedule$"), _reporting_tab(CREATE_OR_EDIT)),
    (("POST", "PUT", "PATCH"), re.compile(r"^/api/scheduled-reports(/|$)"), _reporting_tab(CREATE_OR_EDIT)),
    (None, re.compile(r"^/api/exports(/|$)"), _reporting_tab(SHOW)),
    (None, re.compile(r"^/api/scheduled-reports(/|$)"), _reporting_tab(SHOW)),
    (None, re.compile(r"^/api/reports/enhanced-bank-check/tables$"), _reporting_tab(SHOW)),
]

_CACHE_TTL = float(os.getenv("REPORTING_PERMISSIONS_CACHE_SECONDS", "60"))
_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}

_APPROVAL_ACTIONS = ("approve", "First Approval", "Second Approval")


def _rule_for(method: str, path: str):
    for methods, pattern, need in RULES:
        if methods and method not in methods:
            continue
        if pattern.search(path):
            return need
    return None


def load_group_permissions(user_id: str) -> Optional[List[Dict[str, Any]]]:
    """
    The caller's group rows from Users -> Groups.permissions.
    [] when the user/group is gone; None only when the lookup itself failed.
    """
    now = time.monotonic()
    hit = _cache.get(user_id)
    if hit and hit[0] > now:
        return hit[1]

    try:
        from config.settings import get_db_connection, DATABASE_CONFIG

        db_name = (DATABASE_CONFIG.get("database") or "").strip()
        if not db_name:
            logger.warning("[PagePermissions] DB_NAME empty; cannot load permissions")
            return None
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT g.permissions
                FROM [{db_name}].dbo.Users u
                JOIN [{db_name}].dbo.Groups g ON g.id = u.groupId
                WHERE u.id = %s
                  AND u.deletedAt IS NULL
                  AND g.deletedAt IS NULL
                """,
                (user_id,),
            )
            row = cur.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.warning("[PagePermissions] lookup failed for user_id=%s: %s", user_id, e)
        return None

    rows: List[Dict[str, Any]] = []
    raw = row[0] if row else None
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                rows = [r for r in parsed if isinstance(r, dict)]
        except ValueError:
            logger.warning("[PagePermissions] Groups.permissions is not JSON for user_id=%s", user_id)
    _cache[user_id] = (now + _CACHE_TTL, rows)
    return rows


def _find_row(rows: List[Dict[str, Any]], page: str) -> Optional[Dict[str, Any]]:
    # Same matching as the Node PermissionsGuard: exact page, or one of a comma-separated list.
    for row in rows:
        p = row.get("page")
        if p == page:
            return row
        if isinstance(p, str) and page in [x.strip() for x in p.split(",")]:
            return row
    return None


def _row_allows(row: Optional[Dict[str, Any]], actions: Sequence[str]) -> bool:
    if not row:
        return False
    for action in actions:
        if row.get(action) is True:
            return True
        if action in _APPROVAL_ACTIONS and any(row.get(a) is True for a in _APPROVAL_ACTIONS):
            return True
    return False


def permits(rows: List[Dict[str, Any]], any_of: List[Requirement]) -> bool:
    return any(
        all(_row_allows(_find_row(rows, page), actions) for page, actions in alternative)
        for alternative in any_of
    )


def _forbidden() -> JSONResponse:
    return JSONResponse(
        status_code=403,
        content={"success": False, "message": "You do not have permission to perform this action."},
    )


class PagePermissionMiddleware(BaseHTTPMiddleware):
    """Runs after JWTAuthMiddleware (registered before it, so it sits inside it)."""

    async def dispatch(self, request: Request, call_next):
        method = request.method.upper()
        path = request.url.path
        if method == "OPTIONS" or any(path.startswith(p) for p in PUBLIC_PATHS):
            return await call_next(request)

        need = _rule_for(method, path)
        if need is None:
            logger.warning("[PagePermissions] 403 unmapped route %s %s", method, path)
            return _forbidden()
        if need == LOGIN_ONLY:
            return await call_next(request)

        _, claims = resolve_jwt_token_and_payload(request)
        claims = claims or {}

        if need == SUPER_ADMIN:
            uid = claims.get("id") or claims.get("sub")
            if is_reporting_admin(uid, claims.get("groupName"), claims):
                return await call_next(request)
            return _forbidden()

        user_id = claims.get("id") or claims.get("sub")
        rows = load_group_permissions(str(user_id)) if user_id else None
        if rows is None:
            # Lookup failed: fall back to the rows embedded in a reporting token, if any.
            embedded = claims.get("permissions")
            rows = [r for r in embedded if isinstance(r, dict)] if isinstance(embedded, list) else []

        if permits(rows, need):
            return await call_next(request)
        logger.info("[PagePermissions] 403 user=%s %s %s", user_id, method, path)
        return _forbidden()
