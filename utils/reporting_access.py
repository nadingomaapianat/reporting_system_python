"""
Reporting data access helpers — aligned with reporting_system_node2
`UserFunctionAccessService.isAdmin`.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


def _norm_str(value: Optional[Any]) -> Optional[str]:
    """Strip; empty string becomes None (avoids blocking claim fallbacks)."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _truthy(value: Any) -> bool:
    """Coerce JWT booleans that sometimes arrive as strings (\"true\", \"1\")."""
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, (int, float)) and value == 1:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return False


def _group_matches_super_admin(group: Optional[str]) -> bool:
    """
    True if the group string means "super admin" in the reporting sense.

    Main backend / LDAP may send labels like:
    - super_admin_   (legacy, exact)
    - Super Admin, super admin, SUPER_ADMIN (same meaning, different formatting)

    Node only checks exact 'super_admin_'; we accept common variants so Python
    matches what operators expect when they say "super admin user".
    """
    if not group:
        return False
    s = str(group).strip()
    if s == "super_admin_":
        return True
    # Compare alphanumeric-only: "Super Admin" -> "superadmin"
    alnum = "".join(ch.lower() for ch in s if ch.isalnum())
    return alnum == "superadmin"


def is_reporting_admin(
    user_id: Optional[str],
    group_name: Optional[str],
    claims: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    True if the user should bypass function scoping (same rules as Node):

    - groupName / group / group_name → super admin (see _group_matches_super_admin)
    - user id in REPORTING_SUPER_ADMIN_USER_IDS (comma-separated)
    - role == 'admin' (case-insensitive; JWT claim from Node: `role`)
    - isAdmin truthy (bool or string; JWT claim from Node: `isAdmin`)
    - optional REPORTING_EXTRA_SUPER_ADMIN_GROUP_NAMES=comma-separated exact strings
    """
    c = claims or {}
    gn = (
        _norm_str(group_name)
        or _norm_str(c.get("groupName"))
        or _norm_str(c.get("group"))
        or _norm_str(c.get("group_name"))
    )
    if _group_matches_super_admin(gn):
        return True

    extra = (os.getenv("REPORTING_EXTRA_SUPER_ADMIN_GROUP_NAMES") or "").split(",")
    extra = [x.strip() for x in extra if x.strip()]
    if gn and gn in extra:
        return True

    allowed = (os.getenv("REPORTING_SUPER_ADMIN_USER_IDS") or "").split(",")
    allowed = [x.strip() for x in allowed if x.strip()]
    uid = _norm_str(user_id)
    if uid and uid in allowed:
        return True

    role = c.get("role")
    if role is not None:
        r = str(role).strip().lower()
        if r == "admin":
            return True

    is_admin = c.get("isAdmin")
    if is_admin is None:
        is_admin = c.get("is_admin")
    if _truthy(is_admin):
        return True

    return False
