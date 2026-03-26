"""
Mirror Node `order-by-function.ts`: sort API/export payloads by function-related keys (A→Z).
Query params: orderByFunction, orderByFunctionAsc — truthy: 1, true, yes (case-insensitive).
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, TypeVar

from starlette.requests import Request

T = TypeVar("T")


def order_by_function_from_request(request: Optional[Request]) -> bool:
    if request is None:
        return False
    q = getattr(request, "query_params", None)
    if q is None:
        return False
    v = q.get("orderByFunction") or q.get("orderByFunctionAsc")
    if v is True or v == 1:
        return True
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes")


_SORT_KEYS = (
    "function_name",
    "functionName",
    "function",
    "Function",
    "incident_department",
    "department",
    "Department",
    "function_id",
    "functionId",
    "Functionn",
    "FunctionID",
    "name",
)


def _sort_key(row: Dict[str, Any]) -> str:
    for k in _SORT_KEYS:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def sort_rows_by_function_asc(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return rows
    return sorted(rows, key=lambda r: _sort_key(r).lower())


def _is_plain_object(val: Any) -> bool:
    return isinstance(val, dict)


def apply_order_by_function_deep(input_val: T) -> T:
    """Recursively sort arrays of plain dicts by function/name keys."""

    def walk(val: Any) -> Any:
        if val is None or not isinstance(val, (dict, list)):
            return val
        if isinstance(val, list):
            inner = [walk(x) for x in val]
            if inner and all(_is_plain_object(x) for x in inner):
                return sort_rows_by_function_asc([copy.deepcopy(x) for x in inner])
            return inner
        if _is_plain_object(val):
            return {k: walk(val[k]) for k in val}
        return val

    return walk(copy.deepcopy(input_val))


def sort_paginated_response_if_needed(result: T, enabled: bool) -> T:
    if not enabled or not isinstance(result, dict):
        return result
    data = result.get("data")
    if not isinstance(data, list) or not data:
        return result
    out = dict(result)
    out["data"] = sort_rows_by_function_asc([dict(x) if isinstance(x, dict) else x for x in data])
    return out  # type: ignore[return-value]
