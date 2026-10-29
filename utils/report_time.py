"""
Time printed on reports ("Generated on", "Report Date"): the bank's local time, not the server clock.
The container clock is UTC, so a report made at 16:24 in Cairo said 13:24.
REPORT_TIMEZONE overrides the zone (IANA name, default Africa/Cairo; Egypt's DST is handled).
"""
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


def report_now() -> datetime:
    name = (os.getenv("REPORT_TIMEZONE") or "Africa/Cairo").strip()
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(name))
        except Exception:
            pass
    return datetime.now()
