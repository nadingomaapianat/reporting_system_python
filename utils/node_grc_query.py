"""
Normalize GRC query params before calling the Node API so Python sends the same
values Node uses after its controller/service validation (filters in SQL match the UI).

References:
- GrcComplyController.getAllReports: norm(), ISO date prefix, non-empty functionId
- BaseDashboardService.isValidDateString / buildDateFilter (controls, risks, etc.)
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

_ISO_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}")


def norm_grc_query_string(s: Optional[str]) -> Optional[str]:
    """Mirror grc-comply.controller.ts getAllReports `norm` (plus -> space, trim, collapse spaces)."""
    if s is None or not isinstance(s, str):
        return None
    t = s.replace("+", " ").strip()
    t = re.sub(r"\s+", " ", t)
    return t or None


def grc_iso_date_param(s: Optional[str]) -> Optional[str]:
    """
    Valid start/end date for Node GRC dashboards: same rule as
    BaseDashboardService.isValidDateString and comply /all (YYYY-MM-DD prefix after norm).
    """
    if not s:
        return None
    t = norm_grc_query_string(s)
    if not t or len(t) < 10 or not _ISO_PREFIX.match(t):
        return None
    return t


def grc_function_id_param(s: Optional[str]) -> Optional[str]:
    """Non-empty function id after norm (matches comply /all `id && id.length > 0`)."""
    t = norm_grc_query_string(s) if s else None
    return t if t else None


def grc_parse_selected_function_ids_list(
    function_id: Optional[str],
    function_ids_csv: Optional[str],
) -> Optional[List[str]]:
    """
    Parsed list of function ids for SQL/exports (same rules as Node parseGrcFunctionIdsFromQueries).
    Returns None when no explicit filter; non-empty list when user selected one or more functions.
    """
    m = grc_merge_function_query_params(function_id, function_ids_csv)
    if not m:
        return None
    if "functionId" in m:
        return [m["functionId"]]
    raw = m.get("functionIds") or ""
    parts: List[str] = []
    for p in raw.split(","):
        t = grc_function_id_param(p.strip() if p else None)
        if t:
            parts.append(t)
    return parts if parts else None


def grc_merge_function_query_params(
    function_id: Optional[str],
    function_ids_csv: Optional[str],
) -> Dict[str, str]:
    """
    Match Node `parseGrcFunctionIdsFromQueries`: one id → `functionId`; several → `functionIds` (comma-separated).
    Merges optional single `functionId` with comma-separated `functionIds` (deduped, order preserved).
    """
    seen: List[str] = []
    csv_raw = norm_grc_query_string(function_ids_csv) if function_ids_csv else ""
    if csv_raw:
        for part in csv_raw.split(","):
            t = grc_function_id_param(part.strip() if part else None)
            if t and t not in seen:
                seen.append(t)
    single = grc_function_id_param(function_id)
    if single and single not in seen:
        seen.insert(0, single)
    if not seen:
        return {}
    if len(seen) == 1:
        return {"functionId": seen[0]}
    return {"functionIds": ",".join(seen)}


def comply_filters_matching_node_all(
    start_date: Optional[str],
    end_date: Optional[str],
    function_id: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Same tuple passed to GrcComplyService.runAllReports from getAllReports:
    only ISO-prefixed dates and non-empty functionId after norm.
    """
    start = norm_grc_query_string(start_date) if start_date else None
    end = norm_grc_query_string(end_date) if end_date else None
    fid = norm_grc_query_string(function_id) if function_id else None
    if not (start and _ISO_PREFIX.match(start)):
        start = None
    if not (end and _ISO_PREFIX.match(end)):
        end = None
    if not (fid and len(fid) > 0):
        fid = None
    return start, end, fid
