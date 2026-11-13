# -*- coding: utf-8 -*-
"""Check-Host.net client for Iran filter detection.

Rules (Vira):
  • Domain / SNI  → check-dns on Iran nodes
  • IP address    → first check-tcp on port 22 (Iran nodes only)
                    if ALL Iran nodes report closed/filtered on :22
                    → fall back to check-ping (ICMP)
  • Well-known CDN SNI names are whitelisted (skip DNS)

API result formats (observed 2026):
  TCP ok:   [{"address": "x.x.x.x", "time": 0.12}]
  TCP fail: [{"error": "Connection timed out"}]
  Ping ok:  [[["OK", rtt, ip], ["OK", rtt], ...]]
  DNS ok:   [{"A": ["..."], "AAAA": ["..."], "TTL": n}]
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from urllib.request import Request, urlopen

API = "https://check-host.net"
UA = "ViraBot/1.2 (+check-host)"

DEFAULT_IR_NODES = [
    "ir1.node.check-host.net",
    "ir2.node.check-host.net",
    "ir3.node.check-host.net",
    "ir4.node.check-host.net",
    "ir5.node.check-host.net",
    "ir7.node.check-host.net",
    "ir8.node.check-host.net",
]

TCP_PROBE_PORT = 22

_cache: Dict[str, Tuple[float, bool, str]] = {}
_err_streak = 0
_disabled_until = 0.0


def _http_get(url: str, timeout: float = 20.0) -> dict:
    req = Request(url, headers={"Accept": "application/json", "User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore") or "{}")


def _start_check(kind: str, host: str, nodes: List[str]) -> Optional[str]:
    node_q = "&".join(f"node={quote(n)}" for n in nodes[:8])
    url = f"{API}/check-{kind}?host={quote(host)}&{node_q}"
    data = _http_get(url)
    if not data or data.get("ok") != 1:
        logging.warning("check-host start failed kind=%s host=%s data=%s", kind, host, data)
        return None
    return data.get("request_id")


def _poll_result(request_id: str, tries: int = 10, delay: float = 1.0) -> dict:
    url = f"{API}/check-result/{request_id}"
    last = {}
    for _ in range(tries):
        time.sleep(delay)
        last = _http_get(url) or {}
        if last and all(v is not None for v in last.values()):
            return last
    return last


def _node_ok_tcp(val) -> bool:
    """TCP ok: [{"address","time"}]  fail: [{"error":...}]  legacy: [[1,t]]"""
    try:
        if not isinstance(val, list) or not val:
            return False
        first = val[0]
        if isinstance(first, dict):
            if first.get("error"):
                return False
            if "address" in first or "time" in first:
                return True
            return False
        if isinstance(first, list) and first:
            return first[0] == 1
    except Exception:
        pass
    return False


def _node_ok_ping(val) -> bool:
    """Ping ok: [[["OK", rtt, ip], ...]]"""
    try:
        if not isinstance(val, list) or not val:
            return False
        outer = val[0]
        samples = outer if isinstance(outer, list) else val
        for sample in samples:
            if not isinstance(sample, list) or not sample:
                continue
            status = sample[0]
            if status == "OK" or status == 1:
                return True
            if isinstance(status, (int, float)) and status > 0:
                return True
    except Exception:
        pass
    return False


def _is_filter_sinkhole_ip(ip: str) -> bool:
    """Iran DPI often answers filtered domains with 10.10.34.x (not NXDOMAIN)."""
    ip = (ip or "").strip().lower()
    if not ip:
        return True
    # Classic Iranian filter sinkholes
    if ip.startswith("10.10.34."):
        return True
    # Other non-routable / bogus answers that should not count as "reachable DNS"
    if ip.startswith("10.") or ip.startswith("127.") or ip.startswith("0."):
        return True
    if ip.startswith("192.168.") or ip.startswith("169.254."):
        return True
    if ip == "::" or ip == "::1" or ip.startswith("fc") or ip.startswith("fd") or ip.startswith("fe80:"):
        return True
    return False


def _node_ok_dns(val) -> bool:
    """DNS ok only if at least one public (non-sinkhole) A/AAAA is returned.

    Important: Iranian filtered domains often resolve to 10.10.34.34/35/36
    from IR nodes. That is still an A record — must count as FILTERED.
    """
    try:
        if not isinstance(val, list) or not val:
            return False
        for item in val:
            if isinstance(item, dict):
                addrs = []
                for key in ("A", "AAAA"):
                    v = item.get(key) or []
                    if isinstance(v, list):
                        addrs.extend(str(x) for x in v)
                # any public IP => node can resolve the domain properly
                public = [a for a in addrs if a and not _is_filter_sinkhole_ip(a)]
                if public:
                    return True
                # only sinkhole / empty => filtered
                continue
            if isinstance(item, list) and len(item) >= 2 and item[0] in ("A", "AAAA"):
                # legacy: ["A", "1.2.3.4"] or ["A", ["1.2.3.4", ...]]
                data = item[1]
                candidates = data if isinstance(data, list) else [data]
                if any(not _is_filter_sinkhole_ip(str(c)) for c in candidates if c):
                    return True
    except Exception:
        pass
    return False


def _count_filtered(result: dict, kind: str) -> Tuple[int, int]:
    filtered = total = 0
    for _node, val in (result or {}).items():
        if val is None:
            continue
        total += 1
        try:
            if kind == "tcp":
                ok = _node_ok_tcp(val)
            elif kind == "ping":
                ok = _node_ok_ping(val)
            else:
                ok = _node_ok_dns(val)
            if not ok:
                filtered += 1
        except Exception:
            filtered += 1
    return filtered, total


def _run_one(kind: str, target: str, nodes: List[str]) -> Tuple[Optional[int], Optional[int], str]:
    rid = _start_check(kind, target, nodes)
    if not rid:
        return None, None, "start_failed"
    result = _poll_result(rid)
    filtered, total = _count_filtered(result, kind)
    detail = f"filtered={filtered}/{total} method={kind}"
    if total == 0:
        return None, None, "no_nodes"
    return filtered, total, detail


def _is_ip(host: str) -> bool:
    host = host.strip().strip("[]")
    parts = host.split(".")
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return True
    if ":" in host and all(c in "0123456789abcdefABCDEF:" for c in host):
        return True
    return False


def check_host_sync(
    host: str,
    port: Optional[int] = None,
    *,
    max_fail: int = 1,
    prefer: str = "auto",
    nodes: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    global _err_streak, _disabled_until
    if time.time() < _disabled_until:
        return False, "api_temp_disabled"

    nodes = nodes or DEFAULT_IR_NODES
    host = (host or "").strip().strip("[]")
    if not host:
        return False, "empty_host"

    skip_dns_sni = {
        "www.apple.com", "apple.com", "www.cloudflare.com", "cloudflare.com",
        "www.microsoft.com", "microsoft.com", "www.google.com", "google.com",
        "www.amazon.com", "amazon.com", "www.bing.com", "bing.com",
        "dl.google.com", "storage.yandex.net", "yahoo.com", "www.yahoo.com",
        "www.sciencedirect.com", "sciencedirect.com", "steamcommunity.com",
        "www.amd.com", "amd.com", "telegram.org", "www.telegram.org",
    }
    is_ip = _is_ip(host)

    try:
        if prefer == "dns" or (prefer == "auto" and not is_ip):
            if host.lower() in skip_dns_sni:
                return False, "sni_whitelist"
            filtered, total, detail = _run_one("dns", host, nodes)
            if filtered is None:
                _err_streak += 1
                if _err_streak >= 5:
                    _disabled_until = time.time() + 600
                    logging.warning("check-host temporarily disabled after errors")
                return False, detail
            _err_streak = 0
            if filtered > max_fail:
                return True, detail
            return False, detail

        tcp_target = f"{host}:{TCP_PROBE_PORT}"
        filtered, total, detail = _run_one("tcp", tcp_target, nodes)

        if filtered is None:
            logging.info("check-host TCP:22 start failed for %s → fallback ping", host)
            filtered, total, detail = _run_one("ping", host, nodes)
            if filtered is None:
                _err_streak += 1
                if _err_streak >= 5:
                    _disabled_until = time.time() + 600
                    logging.warning("check-host temporarily disabled after errors")
                return False, detail or "start_failed"
            _err_streak = 0
            detail = detail + " (fallback_ping)"
            if filtered > max_fail:
                return True, detail
            return False, detail

        _err_streak = 0

        if total > 0 and filtered >= total:
            logging.info(
                "check-host TCP:22 all closed for %s (%s) → fallback ping",
                host, detail,
            )
            pf, pt, pdetail = _run_one("ping", host, nodes)
            if pf is not None and pt is not None and pt > 0:
                detail = f"{pdetail} (fallback_after_tcp22_all_closed)"
                if pf > max_fail:
                    return True, detail
                return False, detail
            detail = detail + " (ping_fallback_failed)"
            if filtered > max_fail:
                return True, detail
            return False, detail

        if filtered > max_fail:
            return True, detail
        return False, detail

    except Exception as e:
        _err_streak += 1
        logging.warning("check-host error (%s): %s", _err_streak, e)
        if _err_streak >= 5:
            _disabled_until = time.time() + 600
        return False, f"error:{e}"


async def check_host_async(
    host: str,
    port: Optional[int] = None,
    *,
    max_fail: int = 1,
    prefer: str = "auto",
    cache_ttl: int = 3600,
) -> Tuple[bool, str]:
    key = f"v2:{prefer}:{host}"
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < cache_ttl:
        return hit[1], hit[2] + " (cache)"

    loop = asyncio.get_event_loop()
    rejected, detail = await loop.run_in_executor(
        None, lambda: check_host_sync(host, port, max_fail=max_fail, prefer=prefer)
    )
    _cache[key] = (now, rejected, detail)
    if len(_cache) > 5000:
        for k in list(_cache.keys())[:1000]:
            _cache.pop(k, None)
    return rejected, detail
