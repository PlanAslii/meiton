# -*- coding: utf-8 -*-
"""
Xray-based real connectivity tester for VLESS / VMess / Trojan / Shadowsocks share links.
Improved: empty fp cleanup, reality validation, xhttp/httpupgrade map, reliable process kill,
prefer XRAY_BIN=/data/xray, shorter startup, clean TCP fallback signal.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import re
import shutil
import tempfile
from typing import Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

logger = logging.getLogger(__name__)

TEST_SOCKS_HOST = "127.0.0.1"
TEST_HTTP_URL = os.environ.get("XRAY_TEST_URL", "http://www.gstatic.com/generate_204")
XRAY_BIN_CANDIDATES = [
    os.environ.get("XRAY_BIN", ""),
    "/data/xray",
    "./xray",
    "./bin/xray",
    "/usr/local/bin/xray",
    "/usr/bin/xray",
    shutil.which("xray") or "",
]


def find_xray_binary() -> Optional[str]:
    for path in XRAY_BIN_CANDIDATES:
        if path and os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return None


def xray_available() -> bool:
    return find_xray_binary() is not None


def _b64decode_padded(data: str) -> bytes:
    data = data.strip()
    pad = (-len(data)) % 4
    return base64.b64decode(data + ("=" * pad))


def _parse_qs_flat(query: str) -> dict:
    qs = parse_qs(query, keep_blank_values=True)
    return {k: (v[0] if v else "") for k, v in qs.items()}


def _clean_fp(fp: str) -> str:
    fp = (fp or "").strip()
    if not fp or fp in ("None", "null", "undefined"):
        return "chrome"
    return fp


def _norm_network(net: str) -> str:
    net = (net or "tcp").lower().strip()
    if net in ("websocket",):
        return "ws"
    # map newer aliases to closest supported stream
    if net in ("xhttp", "httpupgrade", "http"):
        return "ws" if net != "http" else "tcp"
    if net in ("h2", "http2"):
        return "h2"
    return net


def share_link_to_outbound(link: str) -> Optional[dict]:
    try:
        link = (link or "").strip()
        if link.startswith("vmess://"):
            return _vmess_to_outbound(link)
        if link.startswith("vless://"):
            return _vless_to_outbound(link)
        if link.startswith("trojan://"):
            return _trojan_to_outbound(link)
        if link.startswith("ss://"):
            return _ss_to_outbound(link)
    except Exception as e:
        logger.warning("share_link_to_outbound failed: %s", e)
    return None


def _vmess_to_outbound(link: str) -> Optional[dict]:
    raw = link[8:]
    cfg = json.loads(_b64decode_padded(raw.split("?")[0]).decode("utf-8", "ignore"))
    add = cfg.get("add") or cfg.get("host")
    port = int(cfg.get("port") or 0)
    uuid = cfg.get("id")
    if not add or not port or not uuid:
        return None

    network = _norm_network(cfg.get("net") or "tcp")
    security = (cfg.get("tls") or "").lower()
    if security in ("tls", "reality"):
        sec = security
    else:
        sec = "none"

    stream = {"network": network, "security": sec if sec != "none" else "none"}
    if network == "ws":
        stream["wsSettings"] = {
            "path": cfg.get("path") or "/",
            "headers": {"Host": cfg.get("host") or cfg.get("sni") or add},
        }
    if network == "grpc":
        stream["grpcSettings"] = {"serviceName": cfg.get("path") or cfg.get("serviceName") or ""}
    if network == "tcp":
        header_type = (cfg.get("type") or "none").lower()
        if header_type and header_type not in ("none", ""):
            stream["tcpSettings"] = {"header": {"type": header_type}}
    if sec == "tls":
        stream["tlsSettings"] = {
            "serverName": cfg.get("sni") or cfg.get("host") or add,
            "fingerprint": _clean_fp(cfg.get("fp")),
            "allowInsecure": str(cfg.get("allowInsecure") or cfg.get("ain") or "0") in ("1", "true", "True"),
        }
        if cfg.get("alpn"):
            stream["tlsSettings"]["alpn"] = [x.strip() for x in str(cfg.get("alpn")).split(",") if x.strip()]
    if sec == "reality":
        pbk = cfg.get("pbk") or ""
        sni = cfg.get("sni") or cfg.get("host") or add
        if not pbk:
            return None
        stream["realitySettings"] = {
            "serverName": sni,
            "fingerprint": _clean_fp(cfg.get("fp")),
            "publicKey": pbk,
            "shortId": cfg.get("sid") or "",
            "spiderX": cfg.get("spx") or "",
        }

    return {
        "tag": "proxy",
        "protocol": "vmess",
        "settings": {
            "vnext": [{
                "address": add,
                "port": port,
                "users": [{
                    "id": uuid,
                    "alterId": int(cfg.get("aid") or 0),
                    "security": cfg.get("scy") or "auto",
                }],
            }]
        },
        "streamSettings": stream,
    }


def _vless_to_outbound(link: str) -> Optional[dict]:
    u = urlparse(link)
    uuid = unquote(u.username or "")
    host = u.hostname
    port = u.port or 443
    if not uuid or not host:
        return None
    q = _parse_qs_flat(u.query)

    network = _norm_network(q.get("type") or q.get("network") or "tcp")
    security = (q.get("security") or "none").lower()
    if security not in ("none", "tls", "reality"):
        security = "none"

    stream = {"network": network, "security": security}
    if network == "ws":
        stream["wsSettings"] = {
            "path": q.get("path") or "/",
            "headers": {"Host": q.get("host") or q.get("sni") or host},
        }
    if network == "grpc":
        stream["grpcSettings"] = {
            "serviceName": q.get("serviceName") or q.get("path") or "",
            "multiMode": q.get("mode") == "multi",
        }
    if network == "tcp":
        header_type = (q.get("headerType") or "none").lower()
        if header_type and header_type not in ("none", ""):
            stream["tcpSettings"] = {"header": {"type": header_type}}
    if network == "h2":
        stream["httpSettings"] = {
            "path": q.get("path") or "/",
            "host": [q.get("host") or q.get("sni") or host],
        }

    if security == "tls":
        stream["tlsSettings"] = {
            "serverName": q.get("sni") or host,
            "fingerprint": _clean_fp(q.get("fp")),
            "allowInsecure": q.get("allowInsecure") in ("1", "true", "True") or q.get("insecure") in ("1", "true", "True"),
        }
        if q.get("alpn"):
            stream["tlsSettings"]["alpn"] = [x.strip() for x in q["alpn"].split(",") if x.strip()]

    if security == "reality":
        pbk = (q.get("pbk") or "").strip()
        sni = (q.get("sni") or q.get("host") or host or "").strip()
        if not pbk or not sni:
            logger.debug("reality missing pbk/sni — skip xray test")
            return None
        stream["realitySettings"] = {
            "serverName": sni,
            "fingerprint": _clean_fp(q.get("fp")),
            "publicKey": pbk,
            "shortId": q.get("sid") or "",
            "spiderX": q.get("spx") or "",
        }

    user = {"id": uuid, "encryption": q.get("encryption") or "none"}
    flow = (q.get("flow") or "").strip()
    if flow:
        user["flow"] = flow

    return {
        "tag": "proxy",
        "protocol": "vless",
        "settings": {"vnext": [{"address": host, "port": int(port), "users": [user]}]},
        "streamSettings": stream,
    }


def _trojan_to_outbound(link: str) -> Optional[dict]:
    u = urlparse(link)
    password = unquote(u.username or "")
    host = u.hostname
    port = u.port or 443
    if not password or not host:
        return None
    q = _parse_qs_flat(u.query)
    network = _norm_network(q.get("type") or "tcp")
    security = (q.get("security") or "tls").lower()
    if security not in ("tls", "reality", "none"):
        security = "tls"
    stream = {"network": network, "security": security}
    if network == "ws":
        stream["wsSettings"] = {
            "path": q.get("path") or "/",
            "headers": {"Host": q.get("host") or q.get("sni") or host},
        }
    if network == "grpc":
        stream["grpcSettings"] = {"serviceName": q.get("serviceName") or q.get("path") or ""}
    if security == "tls":
        stream["tlsSettings"] = {
            "serverName": q.get("sni") or host,
            "fingerprint": _clean_fp(q.get("fp")),
            "allowInsecure": q.get("allowInsecure") in ("1", "true", "True"),
        }
    if security == "reality":
        pbk = (q.get("pbk") or "").strip()
        sni = (q.get("sni") or host).strip()
        if not pbk:
            return None
        stream["realitySettings"] = {
            "serverName": sni,
            "fingerprint": _clean_fp(q.get("fp")),
            "publicKey": pbk,
            "shortId": q.get("sid") or "",
            "spiderX": q.get("spx") or "",
        }
    return {
        "tag": "proxy",
        "protocol": "trojan",
        "settings": {"servers": [{"address": host, "port": int(port), "password": password}]},
        "streamSettings": stream,
    }


def _ss_to_outbound(link: str) -> Optional[dict]:
    raw = link[5:].split("#")[0].split("?")[0]
    method = password = host = None
    port = 0
    try:
        if "@" in raw and not raw.startswith("@"):
            userinfo, hostport = raw.split("@", 1)
            try:
                decoded = _b64decode_padded(userinfo).decode()
                if ":" in decoded:
                    method, password = decoded.split(":", 1)
                else:
                    method, password = "aes-256-gcm", decoded
            except Exception:
                method, password = userinfo.split(":", 1) if ":" in userinfo else ("aes-256-gcm", userinfo)
            host, port_s = hostport.rsplit(":", 1)
            port = int(re.match(r"\d+", port_s).group(0))
        else:
            decoded = _b64decode_padded(raw).decode()
            if "@" in decoded:
                userinfo, hostport = decoded.split("@", 1)
                method, password = userinfo.split(":", 1)
                host, port_s = hostport.rsplit(":", 1)
                port = int(re.match(r"\d+", port_s).group(0))
    except Exception as e:
        logger.warning("SS parse error: %s", e)
        return None
    if not all([method, password, host, port]):
        return None
    return {
        "tag": "proxy",
        "protocol": "shadowsocks",
        "settings": {
            "servers": [{
                "address": host,
                "port": int(port),
                "method": method,
                "password": password,
            }]
        },
    }


def build_xray_config(outbound: dict, socks_port: int) -> dict:
    return {
        "log": {"loglevel": "none"},
        "inbounds": [{
            "tag": "socks-in",
            "listen": TEST_SOCKS_HOST,
            "port": socks_port,
            "protocol": "socks",
            "settings": {"udp": False, "auth": "noauth"},
        }],
        "outbounds": [
            outbound,
            {"tag": "direct", "protocol": "freedom"},
            {"tag": "block", "protocol": "blackhole"},
        ],
        "routing": {
            "domainStrategy": "AsIs",
            "rules": [{"type": "field", "outboundTag": "proxy", "network": "tcp,udp"}],
        },
    }


async def _kill_proc(proc) -> None:
    if proc is None:
        return
    try:
        if proc.returncode is None:
            proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except Exception:
                pass
    except Exception:
        pass


async def test_with_xray(link: str, timeout: float = 8.0) -> Tuple[bool, int]:
    """
    Returns (is_online, ping_ms).
    ping_ms = -1 on failure.
    """
    xray_bin = find_xray_binary()
    if not xray_bin:
        return False, -1

    outbound = share_link_to_outbound(link)
    if not outbound:
        return False, -1

    # unique-ish port to reduce collision under concurrency=2
    socks_port = random.randint(19000, 45000)
    cfg = build_xray_config(outbound, socks_port)

    tmp_dir = tempfile.mkdtemp(prefix="xray_test_")
    cfg_path = os.path.join(tmp_dir, "config.json")
    proc = None
    try:
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f)

        proc = await asyncio.create_subprocess_exec(
            xray_bin, "run", "-c", cfg_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

        # short boot wait
        await asyncio.sleep(0.35)
        if proc.returncode is not None:
            return False, -1

        t_limit = max(3.0, min(float(timeout), 12.0))

        try:
            import aiohttp
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(f"socks5://{TEST_SOCKS_HOST}:{socks_port}")
            start = asyncio.get_event_loop().time()
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    TEST_HTTP_URL,
                    timeout=aiohttp.ClientTimeout(total=t_limit),
                    allow_redirects=True,
                ) as resp:
                    if resp.status in (200, 204, 301, 302, 404):
                        # 404 from generate_204 still means tunnel worked on some nets
                        ping = int((asyncio.get_event_loop().time() - start) * 1000)
                        return True, max(ping, 1)
                    return False, -1
        except ImportError:
            curl = shutil.which("curl")
            if not curl:
                return False, -1
            start = asyncio.get_event_loop().time()
            curl_proc = await asyncio.create_subprocess_exec(
                curl, "-s", "-o", "/dev/null", "-w", "%{http_code}",
                "--socks5-hostname", f"{TEST_SOCKS_HOST}:{socks_port}",
                "--max-time", str(int(t_limit)),
                TEST_HTTP_URL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                stdout, _ = await asyncio.wait_for(curl_proc.communicate(), timeout=t_limit + 1)
            except asyncio.TimeoutError:
                try:
                    curl_proc.kill()
                except Exception:
                    pass
                return False, -1
            code = (stdout or b"").decode().strip()
            if code in ("200", "204", "301", "302", "404"):
                ping = int((asyncio.get_event_loop().time() - start) * 1000)
                return True, max(ping, 1)
            return False, -1
        except Exception:
            return False, -1
    finally:
        await _kill_proc(proc)
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass


async def test_with_tcp(ip: str, port: int, timeout: float = 2.0) -> Tuple[bool, int]:
    if not ip or not port:
        return False, -1
    try:
        start = asyncio.get_event_loop().time()
        _, writer = await asyncio.wait_for(asyncio.open_connection(ip, int(port)), timeout=timeout)
        ping = int((asyncio.get_event_loop().time() - start) * 1000)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, max(ping, 1)
    except Exception:
        return False, -1


def auto_update_xray(target_path: str = None) -> dict:
    """Download latest Xray-linux-64 from GitHub releases into target_path."""
    import urllib.request
    import zipfile
    import stat

    target = target_path or os.environ.get("XRAY_BIN") or "/data/xray"
    result = {"ok": False, "path": target, "version": None, "error": None, "updated": False}
    try:
        url = None
        version = None
        try:
            req = urllib.request.Request(
                "https://api.github.com/repos/XTLS/Xray-core/releases/latest",
                headers={"User-Agent": "daily-config-bot"},
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read().decode())
            version = data.get("tag_name", "")
            for a in data.get("assets", []):
                if a.get("name") == "Xray-linux-64.zip":
                    url = a.get("browser_download_url")
                    break
        except Exception as e:
            logger.warning("Xray release API failed: %s", e)

        if not url:
            version = version or "v26.3.27"
            url = f"https://github.com/XTLS/Xray-core/releases/download/{version}/Xray-linux-64.zip"

        result["version"] = version
        parent = os.path.dirname(os.path.abspath(target)) or "."
        os.makedirs(parent, exist_ok=True)

        tmp_zip = tempfile.mktemp(suffix=".zip")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 daily-config-bot"})
        with urllib.request.urlopen(req, timeout=180) as r, open(tmp_zip, "wb") as f:
            f.write(r.read())

        with zipfile.ZipFile(tmp_zip) as z:
            member = next((n for n in z.namelist() if n == "xray" or n.endswith("/xray")), None)
            if not member:
                raise RuntimeError("xray binary not found in zip")
            data = z.read(member)

        tmp_bin = target + ".new"
        with open(tmp_bin, "wb") as f:
            f.write(data)
        os.chmod(tmp_bin, 0o755)
        os.replace(tmp_bin, target)
        try:
            os.remove(tmp_zip)
        except Exception:
            pass

        result["ok"] = True
        result["updated"] = True
        return result
    except Exception as e:
        result["error"] = str(e)
        return result
