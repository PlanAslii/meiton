"""
Vira quality helpers:
- identifier / pattern blacklist (e.g. MiTiVPN variants)
- per-source hourly NPVT cap
- weekly source quality aggregation helpers
"""
from __future__ import annotations

import re
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# in-memory: source_id -> list of monotonic timestamps of queued NPVT files
_npvt_hourly: Dict[str, List[float]] = defaultdict(list)


def normalize_pattern(p: str) -> str:
    return (p or "").strip().lower()


def parse_blacklist(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[\n,;]+", raw)
    out = []
    for p in parts:
        p = normalize_pattern(p)
        if p and p not in out:
            out.append(p)
    return out


def link_hits_blacklist(link: str, patterns: List[str]) -> Optional[str]:
    """
    Smart match for spammy configs that change UUID/host but keep brand markers.
    Checks full link, fragment/name, userinfo (password), and host.
    """
    if not link or not patterns:
        return None
    low = link.lower()
    # extract pieces
    name = ""
    host = ""
    userinfo = ""
    try:
        if "#" in link:
            name = link.split("#", 1)[1]
        if "://" in link:
            rest = link.split("://", 1)[1]
            main = rest.split("#")[0].split("?")[0]
            if "@" in main:
                userinfo, hostport = main.rsplit("@", 1)
            else:
                hostport = main
            host = hostport.split(":")[0]
    except Exception:
        pass
    hay = " ".join([low, name.lower(), host.lower(), userinfo.lower()])
    for p in patterns:
        if not p:
            continue
        if p in hay:
            return p
        # compact alphanumeric compare (mitivpn vs MiTi-VPN)
        compact_p = re.sub(r"[^a-z0-9]", "", p)
        compact_h = re.sub(r"[^a-z0-9]", "", hay)
        if compact_p and len(compact_p) >= 4 and compact_p in compact_h:
            return p
    return None


def source_npvt_allowed(source_id: str, max_per_hour: int) -> bool:
    """Return True if this source may queue another NPVT file."""
    if max_per_hour <= 0:
        return True
    key = str(source_id)
    now = time.time()
    window = _npvt_hourly[key]
    # drop older than 1h
    _npvt_hourly[key] = [t for t in window if now - t < 3600]
    if len(_npvt_hourly[key]) >= max_per_hour:
        return False
    return True


def source_npvt_record(source_id: str) -> None:
    _npvt_hourly[str(source_id)].append(time.time())


def source_npvt_count(source_id: str) -> int:
    now = time.time()
    key = str(source_id)
    _npvt_hourly[key] = [t for t in _npvt_hourly[key] if now - t < 3600]
    return len(_npvt_hourly[key])
