"""
Reporting data access helpers — aligned with reporting_system_node2
`UserFunctionAccessService.isAdmin`.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional


def is_reporting_admin(
    user_id: Optional[str],
    group_name: Optional[str],
    claims: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    True if the user should bypass function scoping (same rules as Node):

    - groupName / group == 'super_admin_'
    - user id in REPORTING_SUPER_ADMIN_USER_IDS (comma-separated)
    - role == 'admin' (JWT claim from Node: `role`)
    - isAdmin == true (JWT claim from Node: `isAdmin`)
    """
    c = claims or {}
    gn = group_name if group_name is not None else (c.get("groupName") or c.get("group"))
    if gn == "super_admin_":
        return True

    allowed = (os.getenv("REPORTING_SUPER_ADMIN_USER_IDS") or "").split(",")
    allowed = [x.strip() for x in allowed if x.strip()]
    if user_id and str(user_id).strip() in allowed:
        return True

    role = c.get("role")
    if role is not None and str(role).lower() == "admin":
        return True

    is_admin = c.get("isAdmin")
    if is_admin is None:
        is_admin = c.get("is_admin")
    if is_admin is True:
        return True

    return False
