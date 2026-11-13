# -*- coding: utf-8 -*-
"""MahsaServer API client — full CRUD for /backend/api/v1/config/"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

MAHSA_API_BASE = os.environ.get(
    "MAHSA_API_BASE", "https://www.mahsaserver.com/backend/api/v1"
).rstrip("/")
DEFAULT_TOKEN = os.environ.get("MAHSA_API_TOKEN", "")


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Token {token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "ViraBot-MahsaPanel/1.0",
    }


def _request_sync(
    method: str,
    endpoint: str,
    token: str,
    payload: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: float = 45.0,
) -> Tuple[bool, Any]:
    url = endpoint
    if params:
        q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None and v != ""})
        if q:
            url = f"{endpoint}?{q}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=_headers(token), method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", errors="replace")
            if r.status == 204 or not body.strip():
                return True, None
            try:
                return True, json.loads(body)
            except Exception:
                return True, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        log.warning("[MAHSA] HTTPError %s %s %s", method, e.code, body[:400])
        try:
            parsed = json.loads(body) if body else {"detail": body}
        except Exception:
            parsed = {"detail": body[:400]}
        return False, {"status": e.code, "error": parsed}
    except Exception as e:
        log.error("[MAHSA] request error: %s", e)
        return False, {"status": 0, "error": str(e)}


def is_soft_error(msg: str) -> bool:
    if not msg:
        return False
    soft = (
        "already used by another donor",
        "same config with different alias",
        "New donors can submit up to",
        "credential",
    )
    low = str(msg).lower()
    return any(s.lower() in low for s in soft)


def normalize_share_url(url: str) -> str:
    return (url or "").strip()


async def _run(method, endpoint, token, payload=None, params=None):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, lambda: _request_sync(method, endpoint, token, payload, params)
    )


async def mahsa_create_config(
    url: str,
    ads_url: str,
    token: str = None,
    pool: str = "mahsa",
    use_mux: bool = False,
    use_fragment: bool = False,
) -> Tuple[bool, str]:
    token = (token or DEFAULT_TOKEN or "").strip()
    if not token:
        return False, "MAHSA token empty"
    if not url or not ads_url:
        return False, "url/ads_url required"
    url = (url or "").strip()
    payload = {
        "url": url[:2000],
        "ads_url": ads_url[:2000],
        "pool": pool or "mahsa",
        "use_mux": bool(use_mux),
        "use_fragment": bool(use_fragment),
    }
    ok, data = await _run("POST", f"{MAHSA_API_BASE}/config/", token, payload)
    if ok:
        return True, json.dumps(data) if not isinstance(data, str) else str(data)
    err = data.get("error", data) if isinstance(data, dict) else data
    return False, f"HTTP {data.get('status', '?')}: {err}" if isinstance(data, dict) else str(data)


async def mahsa_list_configs(
    token: str = None,
    *,
    alias: str = None,
    hash: str = None,
    is_active: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> Tuple[bool, Any]:
    token = (token or DEFAULT_TOKEN or "").strip()
    if not token:
        return False, {"error": "token empty"}
    params = {"limit": limit, "offset": offset}
    if alias:
        params["alias"] = alias
    if hash:
        params["hash"] = hash
    if is_active is not None:
        params["is_active"] = "true" if is_active else "false"
    return await _run("GET", f"{MAHSA_API_BASE}/config/", token, params=params)


async def mahsa_get_config(config_id: str, token: str = None) -> Tuple[bool, Any]:
    token = (token or DEFAULT_TOKEN or "").strip()
    if not token:
        return False, {"error": "token empty"}
    cid = urllib.parse.quote(str(config_id), safe="")
    return await _run("GET", f"{MAHSA_API_BASE}/config/{cid}/", token)


async def mahsa_update_config(
    config_id: str,
    token: str = None,
    *,
    url: str = None,
    ads_url: str = None,
    pool: str = None,
    use_mux: bool = None,
    use_fragment: bool = None,
    partial: bool = True,
) -> Tuple[bool, Any]:
    token = (token or DEFAULT_TOKEN or "").strip()
    if not token:
        return False, {"error": "token empty"}
    payload = {}
    if url is not None:
        payload["url"] = str(url)[:2000]
    if ads_url is not None:
        payload["ads_url"] = str(ads_url)[:2000]
    if pool is not None:
        payload["pool"] = pool
    if use_mux is not None:
        payload["use_mux"] = bool(use_mux)
    if use_fragment is not None:
        payload["use_fragment"] = bool(use_fragment)
    cid = urllib.parse.quote(str(config_id), safe="")
    method = "PATCH" if partial else "PUT"
    return await _run(method, f"{MAHSA_API_BASE}/config/{cid}/", token, payload)


async def mahsa_delete_config(config_id: str, token: str = None) -> Tuple[bool, Any]:
    token = (token or DEFAULT_TOKEN or "").strip()
    if not token:
        return False, {"error": "token empty"}
    cid = urllib.parse.quote(str(config_id), safe="")
    return await _run("DELETE", f"{MAHSA_API_BASE}/config/{cid}/", token)
