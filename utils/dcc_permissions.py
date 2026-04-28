"""
DCC permission rows embedded in JWT (`permissions` array).
Aligned with reporting-node: `findDccPermissionRow` + `dccRowSatisfiesActions`
(`src/auth/utils/dcc-permission-row.ts`).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple


def find_dcc_permission_row(rows: Any, page: str) -> Optional[dict]:
    if not isinstance(rows, list) or not isinstance(page, str) or not page:
        return None
    for p in rows:
        if not p or not isinstance(p, dict):
            continue
        row = p
        if row.get("page") == page:
            return row
        rp = row.get("page")
        if isinstance(rp, str):
            pages = [x.strip() for x in rp.split(",") if x.strip()]
            if page in pages:
                return row
    return None


def dcc_row_satisfies_actions(
    row: Optional[dict], actions: Sequence[str], require_all: bool
) -> bool:
    if not row or not actions:
        return False

    def ok(action: str) -> bool:
        return row.get(action) is True

    if require_all:
        return all(ok(a) for a in actions)
    return any(ok(a) for a in actions)


@dataclass(frozen=True)
class _RouteRule:
    prefix: str
    methods: Optional[frozenset[str]]  # None = any method
    page: str
    actions: Tuple[str, ...]
    require_all: bool


# First matching rule wins (order: more specific prefixes first).
_ROUTE_RULES: Tuple[_RouteRule, ...] = (
    _RouteRule("/api/grc/", None, "Dashboard", ("show",), False),
    _RouteRule("/api/reports/", None, "Dashboard", ("show",), False),
    _RouteRule("/api/exports/", None, "Dashboard", ("show",), False),
    _RouteRule("/parse-template", frozenset({"POST"}), "ICR Templates", ("create", "edit"), True),
    _RouteRule(
        "/word-template",
        frozenset({"POST", "PUT", "PATCH", "DELETE"}),
        "ICR Templates",
        ("create", "edit"),
        True,
    ),
    _RouteRule("/word-template", frozenset({"GET", "HEAD"}), "ICR Templates", ("show",), False),
    _RouteRule("/xbrl/", None, "Dashboard", ("show",), False),
    _RouteRule("/api/auth/", None, "Dashboard", ("show",), False),
)


def resolve_required_permission(path: str, method: str) -> Tuple[str, Tuple[str, ...], bool]:
    """
    Map request path + HTTP method to (DCC page, actions, require_all).
    Default: Dashboard + show (same baseline as most Nest GRC controllers).
    """
    m = (method or "GET").upper()
    for rule in _ROUTE_RULES:
        if not path.startswith(rule.prefix):
            continue
        if rule.methods is not None and m not in rule.methods:
            continue
        return rule.page, rule.actions, rule.require_all
    return "Dashboard", ("show",), False


def assert_user_has_permission(user: dict, path: str, method: str) -> None:
    rows = user.get("permissions")
    if not isinstance(rows, list) or len(rows) == 0:
        raise PermissionError("No DCC permissions in token")

    page, actions, require_all = resolve_required_permission(path, method)
    row = find_dcc_permission_row(rows, page)
    if not row:
        raise PermissionError(f'No permissions configured for page "{page}"')

    if not dcc_row_satisfies_actions(row, actions, require_all):
        need = " and ".join(actions) if require_all else " or ".join(actions)
        raise PermissionError(f"Missing required permission ({need}) on {page}")


def permission_error_response(message: str, status_code: int = 403):
    from starlette.responses import JSONResponse

    return JSONResponse(
        status_code=status_code,
        content={"success": False, "message": message},
    )
