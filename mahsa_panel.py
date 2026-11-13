# -*- coding: utf-8 -*-
"""
Vira · Mahsa Admin Web Panel
OXNET-inspired clean design (light default + dark toggle)
OTP via bot button only — 60s validity; login IP reported to admin
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from typing import Callable, Optional

from aiohttp import web

log = logging.getLogger(__name__)

_panel_password: str = ""
_panel_password_exp: float = 0.0
_SESSION_SECRET = os.environ.get("PANEL_SESSION_SECRET") or secrets.token_hex(32)
_SESSION_TTL = int(os.environ.get("PANEL_SESSION_TTL", "3600"))
_OTP_TTL = int(os.environ.get("PANEL_OTP_TTL", "60"))  # seconds
_notify_login: Optional[Callable] = None  # async (ip, ok, detail)
_notify_request: Optional[Callable] = None  # async (ip, password, ttl)
_is_enabled: Optional[Callable] = None  # () -> bool


def get_current_password() -> str:
    if not _panel_password or time.time() > _panel_password_exp:
        return ""
    return _panel_password


def get_password_remaining() -> int:
    return max(0, int(_panel_password_exp - time.time()))


def request_password(ttl: int = None) -> tuple:
    """Generate OTP valid for ttl seconds. Returns (password, ttl)."""
    global _panel_password, _panel_password_exp
    ttl = int(ttl if ttl is not None else _OTP_TTL)
    _panel_password = secrets.token_urlsafe(8)
    _panel_password_exp = time.time() + ttl
    return _panel_password, ttl


# backward-compatible name used by older bot imports
def rotate_password() -> str:
    pw, _ = request_password()
    return pw


def check_password(pw: str) -> bool:
    pw = (pw or "").strip()
    if not pw or not _panel_password:
        return False
    if time.time() > _panel_password_exp:
        return False
    return hmac.compare_digest(pw, _panel_password)


def _sign(payload: str) -> str:
    return hmac.new(_SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]


def _make_session_token() -> str:
    exp = int(time.time()) + _SESSION_TTL
    nonce = secrets.token_hex(8)
    raw = f"{exp}:{nonce}"
    return f"{raw}:{_sign(raw)}"


def _valid_session(token: Optional[str]) -> bool:
    if not token or token.count(":") < 2:
        return False
    raw, sig = token.rsplit(":", 1)
    if not hmac.compare_digest(_sign(raw), sig):
        return False
    try:
        exp = int(raw.split(":")[0])
    except Exception:
        return False
    return time.time() < exp


def _token_from_request(request: web.Request) -> Optional[str]:
    return request.cookies.get("vira_panel_sess")


def _client_ip(request: web.Request) -> str:
    xf = request.headers.get("X-Forwarded-For") or request.headers.get("X-Real-IP")
    if xf:
        return xf.split(",")[0].strip()
    peer = request.remote
    return peer or "?"


def _require_auth(handler):
    async def wrapper(request: web.Request):
        if not _valid_session(_token_from_request(request)):
            if request.path.startswith("/panel/api/"):
                return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
            raise web.HTTPFound("/panel/login")
        return await handler(request)

    return wrapper


def _mahsa_token(request: web.Request) -> str:
    getter = request.app.get("get_mahsa_token")
    if callable(getter):
        try:
            t = getter()
            if t:
                return t
        except Exception:
            pass
    return (os.environ.get("MAHSA_API_TOKEN") or "").strip()



DISABLED_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>ویرا · پنل غیرفعال</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;700&display=swap" rel="stylesheet">
<style>body{font-family:Vazirmatn,sans-serif;background:#F7F8FA;display:grid;place-items:center;min-height:100vh;margin:0;color:#0F172A}
.card{background:#fff;border:1px solid rgba(15,23,42,.08);border-radius:20px;padding:36px;max-width:400px;text-align:center;box-shadow:0 16px 40px rgba(15,23,42,.06)}
h1{font-size:18px;margin:0 0 8px}p{font-size:13px;color:#64748B;line-height:1.7;margin:0}</style></head>
<body><div class="card"><h1>پنل وب موقتاً غیرفعال است</h1><p>از ربات ادمین می‌توانید دوباره آن را روشن کنید.</p></div></body></html>"""

def panel_is_enabled() -> bool:
    if callable(_is_enabled):
        try:
            return bool(_is_enabled())
        except Exception:
            return True
    return True

# ─── HTML (OXNET-style, light default) ──────────────────────────────────────

SHARED_HEAD = r"""
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@3.19.0/dist/tabler-icons.min.css">
<style>
:root{
  --bg:#F7F8FA;--bg2:#FFFFFF;--bg3:#F1F5F9;--card:#FFFFFF;
  --card-b:rgba(15,23,42,.08);--card-bh:rgba(15,23,42,.18);
  --accent:#111827;--accent2:#2563EB;--accent-d:rgba(37,99,235,.08);
  --green:#059669;--green-bg:rgba(5,150,105,.1);--green-t:#047857;
  --red:#DC2626;--red-bg:rgba(220,38,38,.1);--red-t:#991B1B;
  --amber:#D97706;--t1:#0F172A;--t2:#334155;--t3:#64748B;
  --shadow:0 16px 44px rgba(15,23,42,.06);--sidebar:250px;
}
[data-theme="dark"]{
  --bg:#0B0D10;--bg2:#111418;--bg3:#171B21;--card:#151A20;
  --card-b:rgba(255,255,255,.10);--card-bh:rgba(255,255,255,.22);
  --accent:#F8FAFC;--accent2:#60A5FA;--accent-d:rgba(96,165,250,.12);
  --green:#22C55E;--green-bg:rgba(34,197,94,.12);--green-t:#86EFAC;
  --red:#EF4444;--red-bg:rgba(239,68,68,.14);--red-t:#FCA5A5;
  --amber:#F59E0B;--t1:#F8FAFC;--t2:#CBD5E1;--t3:#94A3B8;
  --shadow:0 16px 40px rgba(0,0,0,.45);
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Vazirmatn',sans-serif;background:var(--bg);color:var(--t1);min-height:100vh;-webkit-font-smoothing:antialiased}
a{color:var(--accent2);text-decoration:none}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-thumb{background:var(--bg3);border-radius:999px}
.bg-grid{position:fixed;inset:0;background-image:linear-gradient(var(--card-b) 1px,transparent 1px),linear-gradient(90deg,var(--card-b) 1px,transparent 1px);background-size:40px 40px;z-index:0;pointer-events:none}
.orb{position:fixed;border-radius:50%;filter:blur(100px);z-index:0;pointer-events:none;animation:float 10s ease-in-out infinite alternate}
.orb-1{width:360px;height:360px;background:rgba(37,99,235,.10);top:-80px;right:-80px}
.orb-2{width:280px;height:280px;background:rgba(168,85,247,.08);bottom:-60px;left:-40px;animation-delay:4s}
@keyframes float{0%{transform:translateY(0)}100%{transform:translateY(-24px)}}
.brand-mark{width:42px;height:42px;border-radius:12px;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,#0f172a,#334155);color:#fff;font-weight:900;box-shadow:0 10px 25px rgba(15,23,42,.22);flex-shrink:0}
.brand-mark.lg{width:56px;height:56px;border-radius:16px;font-size:22px}
.glass{background:var(--card);border:1px solid var(--card-b);box-shadow:var(--shadow);border-radius:20px}
.btn-p{background:linear-gradient(135deg,#2563eb,#1d4ed8);color:#fff;border:0;border-radius:12px;padding:10px 18px;font-size:13px;font-weight:700;font-family:inherit;cursor:pointer;display:inline-flex;align-items:center;gap:8px;box-shadow:0 8px 20px rgba(37,99,235,.22);transition:.15s}
.btn-p:hover{filter:brightness(1.06);transform:translateY(-1px)}
.btn-p:disabled{opacity:.5;cursor:not-allowed}
.btn-s{background:var(--bg3);color:var(--t1);border:1px solid var(--card-b);border-radius:12px;padding:10px 16px;font-size:13px;font-weight:600;font-family:inherit;cursor:pointer;display:inline-flex;align-items:center;gap:8px;transition:.15s}
.btn-s:hover{background:var(--accent-d);border-color:var(--card-bh)}
.btn-d{background:var(--red-bg);color:var(--red-t);border:1px solid rgba(220,38,38,.2);border-radius:10px;padding:7px 12px;font-size:12px;font-weight:700;font-family:inherit;cursor:pointer;display:inline-flex;align-items:center;gap:6px}
.btn-sm{padding:7px 12px;font-size:12px;border-radius:10px}
.ox-input{width:100%;background:var(--bg2);border:1px solid var(--card-b);color:var(--t1);border-radius:12px;padding:11px 14px;outline:none;font-family:inherit;font-size:13px;transition:.15s}
.ox-input:focus{border-color:var(--accent2);box-shadow:0 0 0 3px var(--accent-d)}
label.lbl{display:block;font-size:11px;font-weight:700;color:var(--t2);margin-bottom:6px}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.dot-g{background:var(--green);box-shadow:0 0 8px var(--green)}
.dot-r{background:var(--red)}
.badge{display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:800}
.badge-ok{background:var(--green-bg);color:var(--green-t)}
.badge-no{background:var(--red-bg);color:var(--red-t)}
.toast{position:fixed;bottom:22px;left:50%;transform:translateX(-50%) translateY(40px);background:var(--card);border:1px solid var(--card-b);color:var(--t1);border-radius:14px;padding:12px 18px;font-size:12.5px;font-weight:700;opacity:0;transition:.25s;z-index:100;pointer-events:none;box-shadow:var(--shadow);display:flex;align-items:center;gap:8px}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.toast.bad{border-color:rgba(220,38,38,.3);color:var(--red-t)}
/* login */
.login-wrap{min-height:100vh;display:grid;place-items:center;padding:20px;position:relative;z-index:1}
.login-card{width:min(420px,100%);padding:22px 28px 28px}
.login-hint{display:flex;gap:10px;align-items:flex-start;background:var(--accent-d);border:1px solid rgba(37,99,235,.15);border-radius:12px;padding:12px 14px;margin:18px 0;font-size:12px;color:var(--t2);line-height:1.7}
/* shell */
.shell{display:flex;min-height:100vh;position:relative;z-index:1}
.side{width:var(--sidebar);background:var(--bg2);border-left:1px solid var(--card-b);position:fixed;inset:0 auto 0 0;display:flex;flex-direction:column;z-index:20}
[dir=rtl] .side{left:auto;right:0;border-left:0;border-right:1px solid var(--card-b)}
.side-head{padding:18px;border-bottom:1px solid var(--card-b);display:flex;gap:12px;align-items:center}
.nav{padding:12px;flex:1;overflow:auto}
.nav-sec{font-size:10px;font-weight:800;color:var(--t3);letter-spacing:.08em;padding:10px 12px 6px;text-transform:uppercase}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 14px;border-radius:12px;color:var(--t3);font-size:13px;font-weight:600;cursor:pointer;margin-bottom:2px;transition:.15s;border:0;background:transparent;width:100%;font-family:inherit;text-align:right}
.nav-item:hover{background:var(--accent-d);color:var(--t1)}
.nav-item.active{background:var(--accent-d);color:var(--accent2);font-weight:700}
.side-foot{padding:14px;border-top:1px solid var(--card-b);display:grid;gap:8px}
.main{flex:1;margin-right:var(--sidebar);padding:28px 24px 48px;max-width:1200px;width:100%}
[dir=ltr] .main{margin-right:0;margin-left:var(--sidebar)}
.hero{background:linear-gradient(145deg,var(--card),var(--bg3));border:1px solid var(--card-b);border-radius:24px;padding:26px;margin-bottom:20px;position:relative;overflow:hidden}
.hero:after{content:'';position:absolute;width:240px;height:240px;border-radius:50%;background:radial-gradient(circle,var(--accent-d),transparent 70%);bottom:-80px;left:-60px}
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.kpi{background:var(--card);border:1px solid var(--card-b);border-radius:18px;padding:18px;transition:.15s}
.kpi:hover{border-color:var(--card-bh);transform:translateY(-2px);box-shadow:var(--shadow)}
.kpi-lab{font-size:10px;font-weight:800;color:var(--t3);text-transform:uppercase;letter-spacing:.06em}
.kpi-val{font-size:28px;font-weight:900;margin-top:6px;color:var(--t1)}
.toolbar{display:grid;grid-template-columns:repeat(4,1fr) auto;gap:12px;align-items:end;margin-bottom:16px}
table{width:100%;border-collapse:collapse;font-size:12.5px}
th{text-align:right;padding:12px 14px;background:var(--bg3);color:var(--t3);font-size:11px;font-weight:800;border-bottom:1px solid var(--card-b)}
td{padding:14px;border-bottom:1px solid var(--card-b);vertical-align:top;color:var(--t2)}
tr:hover td{background:rgba(15,23,42,.02)}
[data-theme=dark] tr:hover td{background:rgba(255,255,255,.03)}
.url-box{font-family:ui-monospace,monospace;font-size:11px;background:var(--bg2);border:1px solid var(--card-b);border-radius:10px;padding:8px 10px;direction:ltr;text-align:left;max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.modal-bg{position:fixed;inset:0;background:rgba(15,23,42,.45);backdrop-filter:blur(4px);display:none;place-items:center;z-index:50;padding:16px}
.modal-bg.open{display:grid}
.modal{width:min(520px,100%);background:var(--card);border:1px solid var(--card-b);border-radius:20px;overflow:hidden;box-shadow:var(--shadow)}
.modal-h{padding:14px 18px;background:var(--bg3);border-bottom:1px solid var(--card-b);display:flex;justify-content:space-between;align-items:center;font-weight:800;font-size:13px}
.modal-b{padding:18px;display:grid;gap:14px}
.modal-f{padding:14px 18px;background:var(--bg3);border-top:1px solid var(--card-b);display:flex;justify-content:flex-end;gap:8px}
@media(max-width:960px){
  .side{display:none}
  .main{margin:0!important;padding:16px 12px 40px}
  .kpi-grid{grid-template-columns:1fr 1fr}
  .toolbar{grid-template-columns:1fr 1fr}
}
@media(max-width:560px){.kpi-grid{grid-template-columns:1fr}}
</style>
"""

LOGIN_HTML = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="light">
<head>
<title>ویرا · ورود پنل مهسا</title>
{SHARED_HEAD}
</head>
<body>
<div class="bg-grid"></div><div class="orb orb-1"></div><div class="orb orb-2"></div>
<div class="login-wrap">
  <div class="glass login-card" style="padding-top:22px">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">
      <div class="brand-mark lg"><i class="ti ti-network"></i></div>
      <div>
        <div style="font-weight:900;font-size:16px;line-height:1.2">ویرا · پنل مهسا</div>
        <div style="font-size:11px;color:var(--t3);font-weight:600;margin-top:2px">MahsaServer Admin</div>
      </div>
    </div>
    <h1 style="font-size:20px;font-weight:900;margin:4px 0 8px">ورود امن</h1>
    <p style="font-size:12px;color:var(--t3);line-height:1.7;margin-bottom:18px">رمز یک‌بارمصرف به ربات ادمین ارسال می‌شود.</p>
    <form method="post" action="/panel/login">
      <div style="margin-bottom:16px">
        <label class="lbl">کد OTP</label>
        <input class="ox-input" name="password" type="password" required autofocus placeholder="••••••••" style="text-align:center;letter-spacing:.2em;font-family:ui-monospace,monospace"/>
      </div>
      <button class="btn-p" type="submit" style="width:100%;justify-content:center;padding:13px">
        <span>ورود به پنل</span><i class="ti ti-arrow-left"></i>
      </button>
    </form>
    <button type="button" id="btnReqOtp" class="btn-s" style="width:100%;justify-content:center;margin-top:12px;padding:12px" onclick="requestOtp()">
      <i class="ti ti-key"></i> <span id="btnReqLabel">ارسال رمز به ربات</span>
    </button>
    <div id="reqMsg" style="margin-top:12px;font-size:12px;text-align:center;color:var(--t3);min-height:18px"></div>
    <div style="margin-top:18px;text-align:center;font-size:11px;color:var(--t3)">ویرا · پنل مهسا</div>
    <script>
    async function requestOtp(){{
      const btn=document.getElementById('btnReqOtp');
      const msg=document.getElementById('reqMsg');
      btn.disabled=true;
      document.getElementById('btnReqLabel').textContent='در حال ارسال…';
      msg.textContent='';
      try{{
        const r=await fetch('/panel/api/request-otp',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:'{{}}'}});
        const d=await r.json().catch(()=>({{}}));
        if(!r.ok||d.ok===false) throw new Error(d.error||('HTTP '+r.status));
        msg.style.color='var(--green-t)';
        msg.textContent=d.message||'رمز به ربات ادمین ارسال شد.';
      }}catch(e){{
        msg.style.color='var(--red-t)';
        msg.textContent=String(e.message||e);
      }}finally{{
        btn.disabled=false;
        document.getElementById('btnReqLabel').textContent='ارسال رمز به ربات';
      }}
    }}
    </script>
  </div>
</div>
<script>
try{{const t=localStorage.getItem('vira-panel-theme');if(t)document.documentElement.setAttribute('data-theme',t)}}catch(e){{}}
</script>
</body></html>
"""

DASH_HTML = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="light">
<head>
<title>ویرا · پنل مهسا</title>
{SHARED_HEAD}
</head>
<body>
<div class="bg-grid"></div>
<div class="toast" id="toast"><i class="ti ti-circle-check" style="color:var(--green)"></i><span id="toastMsg"></span></div>

<div class="shell">
  <aside class="side">
    <div class="side-head">
      <div class="brand-mark"><i class="ti ti-network"></i></div>
      <div>
        <div style="font-weight:900;font-size:13px">ویرا · مهسا</div>
        <div style="font-size:10px;color:var(--t3);font-weight:700">مدیریت کانفیگ</div>
      </div>
    </div>
    <div class="nav">
      <div class="nav-sec">منو</div>
      <button class="nav-item active" onclick="loadList()"><i class="ti ti-layout-dashboard"></i> داشبورد</button>
      <button class="nav-item" onclick="loadList()"><i class="ti ti-server"></i> کانفیگ‌ها</button>
      <button class="nav-item" onclick="openCreate()"><i class="ti ti-plus"></i> افزودن کانفیگ</button>
      <div class="nav-sec">سیستم</div>
      <button class="nav-item" onclick="toggleTheme()"><i class="ti ti-sun-moon"></i> تم روشن / تیره</button>
    </div>
    <div class="side-foot">
      <a class="btn-s" href="/panel/logout" style="justify-content:center;color:var(--red-t)"><i class="ti ti-logout"></i> خروج</a>
    </div>
  </aside>

  <main class="main">
    <div style="display:flex;flex-wrap:wrap;justify-content:space-between;gap:12px;margin-bottom:20px;align-items:center">
      <div>
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <h1 style="font-size:22px;font-weight:900">داشبورد ویرا</h1>
          <span class="badge badge-ok"><span class="dot dot-g"></span> آنلاین</span>
        </div>
        <p style="font-size:12px;color:var(--t3);margin-top:4px">مدیریت کانفیگ‌های MahsaServer</p>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn-s btn-sm" onclick="loadList()"><i class="ti ti-refresh"></i> بروزرسانی</button>
        <button class="btn-s btn-sm" onclick="copyAllConfigs()"><i class="ti ti-copy"></i> کپی همه</button>
        <button class="btn-p btn-sm" onclick="openCreate()"><i class="ti ti-plus"></i> کانفیگ جدید</button>
      </div>
    </div>

    <div class="hero">
      <div style="position:relative;z-index:1">
        <div style="font-size:11px;font-weight:800;color:var(--accent2);letter-spacing:.08em;margin-bottom:6px">MAHSA API · VIRA</div>
        <div style="font-size:20px;font-weight:900;margin-bottom:6px">سامانه مدیریت کانفیگ</div>
      </div>
    </div>

    <div class="kpi-grid">
      <div class="kpi"><div class="kpi-lab">کل (صفحه)</div><div class="kpi-val" id="stCount">—</div></div>
      <div class="kpi" style="border-right:3px solid var(--green)"><div class="kpi-lab">فعال</div><div class="kpi-val" id="stActive" style="color:var(--green)">—</div></div>
      <div class="kpi" style="border-right:3px solid var(--red)"><div class="kpi-lab">غیرفعال</div><div class="kpi-val" id="stInactive" style="color:var(--red)">—</div></div>
      <div class="kpi"><div class="kpi-lab">آفست</div><div class="kpi-val" id="stOffset">0</div></div>
    </div>

    <div class="glass" style="padding:16px;margin-bottom:16px">
      <div class="toolbar">
        <div>
          <label class="lbl">وضعیت</label>
          <select id="fActive" class="ox-input">
            <option value="">همه</option>
            <option value="true">فعال</option>
            <option value="false">غیرفعال</option>
          </select>
        </div>
        <div>
          <label class="lbl">Alias</label>
          <input id="fAlias" class="ox-input" placeholder="اختیاری"/>
        </div>
        <div>
          <label class="lbl">Hash</label>
          <input id="fHash" class="ox-input" placeholder="اختیاری"/>
        </div>
        <div>
          <label class="lbl">تعداد</label>
          <input id="fLimit" class="ox-input" type="number" value="30" min="1" max="100"/>
        </div>
        <div style="display:flex;gap:8px">
          <button class="btn-p btn-sm" onclick="state.offset=0;loadList()"><i class="ti ti-filter"></i> فیلتر</button>
          <button class="btn-s btn-sm" onclick="prevPage()">قبلی</button>
          <button class="btn-s btn-sm" onclick="nextPage()">بعدی</button>
        </div>
      </div>
    </div>

    <div class="glass" style="overflow:hidden">
      <div style="overflow:auto">
        <table>
          <thead>
            <tr>
              <th>عنوان / شناسه</th>
              <th>آدرس</th>
              <th style="text-align:center">سلامت</th>
              <th style="text-align:center">وضعیت</th>
              <th style="text-align:center">عملیات</th>
            </tr>
          </thead>
          <tbody id="tbody"><tr><td colspan="5" style="text-align:center;color:var(--t3);padding:28px">در حال بارگذاری…</td></tr></tbody>
        </table>
      </div>
    </div>
  </main>
</div>

<div class="modal-bg" id="modalBg">
  <div class="modal">
    <div class="modal-h">
      <span id="modalTitle">کانفیگ</span>
      <button class="btn-s btn-sm" onclick="closeModal()"><i class="ti ti-x"></i></button>
    </div>
    <div class="modal-b">
      <input type="hidden" id="mId"/>
      <div>
        <label class="lbl">URL کانفیگ</label>
        <textarea id="mUrl" class="ox-input" rows="3" style="direction:ltr;text-align:left;font-family:ui-monospace,monospace;font-size:12px"></textarea>
      </div>
      <div>
        <label class="lbl">Ads URL</label>
        <input id="mAds" class="ox-input" style="direction:ltr;text-align:left"/>
      </div>
      <div>
        <label class="lbl">Pool</label>
        <select id="mPool" class="ox-input">
          <option value="mahsa">mahsa</option>
          <option value="warp">warp</option>
          <option value="popup">popup</option>
          <option value="telegram">telegram</option>
        </select>
      </div>
      <div style="display:flex;gap:18px">
        <label style="display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700;color:var(--t2);cursor:pointer">
          <input type="checkbox" id="mMux"/> use_mux
        </label>
        <label style="display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700;color:var(--t2);cursor:pointer">
          <input type="checkbox" id="mFrag"/> use_fragment
        </label>
      </div>
    </div>
    <div class="modal-f">
      <button class="btn-s" onclick="closeModal()">انصراف</button>
      <button class="btn-p" onclick="saveModal()">ذخیره</button>
    </div>
  </div>
</div>

<script>
const state={{offset:0,limit:30,count:0}};
try{{const t=localStorage.getItem('vira-panel-theme');if(t)document.documentElement.setAttribute('data-theme',t)}}catch(e){{}}
function toggleTheme(){{
  const cur=document.documentElement.getAttribute('data-theme')||'light';
  const next=cur==='dark'?'light':'dark';
  document.documentElement.setAttribute('data-theme',next);
  try{{localStorage.setItem('vira-panel-theme',next)}}catch(e){{}}
  toast(next==='dark'?'تم تیره':'تم روشن');
}}
function toast(msg,bad){{
  const el=document.getElementById('toast');
  document.getElementById('toastMsg').textContent=msg;
  el.className='toast show'+(bad?' bad':'');
  setTimeout(()=>el.classList.remove('show'),2800);
}}
function esc(s){{return String(s||'').replace(/[&<>"'`]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','`':'&#96;'}})[c])}}
function short(s,n=52){{s=String(s||'');return s.length>n?s.slice(0,n)+'…':s}}
async function api(path,opts={{}}){{
  const r=await fetch(path,{{credentials:'same-origin',headers:{{'Content-Type':'application/json',...(opts.headers||{{}})}},...opts}});
  if(r.status===401){{location.href='/panel/login';return null}}
  const data=await r.json().catch(()=>({{}}));
  if(!r.ok||data.ok===false) throw new Error(data.error||data.detail||('HTTP '+r.status));
  return data;
}}
async function loadList(){{
  const limit=parseInt(document.getElementById('fLimit').value||'30',10);
  state.limit=limit;
  const q=new URLSearchParams({{limit:String(limit),offset:String(state.offset)}});
  const act=document.getElementById('fActive').value;
  const alias=document.getElementById('fAlias').value.trim();
  const hash=document.getElementById('fHash').value.trim();
  if(act)q.set('is_active',act);
  if(alias)q.set('alias',alias);
  if(hash)q.set('hash',hash);
  try{{
    const data=await api('/panel/api/configs?'+q.toString());
    if(!data)return;
    const results=data.results||[];
    state.count=data.count||results.length;
    document.getElementById('stCount').textContent=state.count;
    document.getElementById('stOffset').textContent=state.offset;
    let a=0,i=0;results.forEach(r=>{{if(r.is_active)a++;else i++}});
    document.getElementById('stActive').textContent=a;
    document.getElementById('stInactive').textContent=i;
    const tb=document.getElementById('tbody');
    window._lastResults=results;if(!results.length){{tb.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--t3);padding:28px">موردی یافت نشد</td></tr>';return}}
    tb.innerHTML=results.map(r=>{{
      const id=r.id||r.hash||'';
      const alias=r.alias||id||'—';
      const health=fmtHealth(r);
      const badge=r.is_active?'<span class="badge badge-ok"><span class="dot dot-g"></span> فعال</span>':'<span class="badge badge-no"><span class="dot dot-r"></span> غیرفعال</span>';
      const name=alias||'بدون نام';
      return `<tr>
        <td><div style="font-weight:800;color:var(--t1)">${{esc(name)}}</div>
            <div style="font-size:10px;color:var(--t3);font-family:ui-monospace,monospace;margin-top:3px">${{esc(id)}}</div></td>
        <td><div class="url-box" title="${{esc(r.url||'')}}">${{esc(short(r.url))}}</div></td>
        <td style="text-align:center;font-weight:800">${{esc(health)}}</td>
        <td style="text-align:center">${{badge}}</td>
        <td style="text-align:center">
          <div style="display:flex;gap:6px;justify-content:center;flex-wrap:wrap">
            <button class="btn-s btn-sm" onclick='editItem(${{JSON.stringify(r)}})'><i class="ti ti-edit"></i> ویرایش</button>
            <button class="btn-d" onclick="delItem('${{esc(id)}}','${{esc(name)}}')"><i class="ti ti-trash"></i> حذف</button>
          </div>
        </td>
      </tr>`;
    }}).join('');
    window._lastResults=results;
  }}catch(e){{toast(String(e.message||e),true)}}
}}
function fmtHealth(r){{
  let h=r.health_status_percent;
  if(h!=null&&typeof h==='object'){{
    try{{h=h.percent??h.score??h.status??JSON.stringify(h)}}catch(e){{h='—'}}
  }}
  if(h==null||h===''||h===undefined){{
    let x=r.xray_result;
    if(x!=null&&typeof x==='object'){{
      try{{x=x.status??x.result??x.message??''}}catch(e){{x=''}}
    }}
    h=(x===''||x==null)?'—':x;
  }}
  return String(h);
}}
function prevPage(){{state.offset=Math.max(0,state.offset-state.limit);loadList()}}
function nextPage(){{state.offset=state.offset+state.limit;loadList()}}
function openCreate(){{
  document.getElementById('modalTitle').textContent='افزودن کانفیگ';
  document.getElementById('mId').value='';
  document.getElementById('mUrl').value='';
  document.getElementById('mAds').value='';
  document.getElementById('mPool').value='mahsa';
  document.getElementById('mMux').checked=false;
  document.getElementById('mFrag').checked=false;
  document.getElementById('modalBg').classList.add('open');
}}
function editItem(r){{
  document.getElementById('modalTitle').textContent='ویرایش کانفیگ';
  document.getElementById('mId').value=r.id||r.hash||'';
  document.getElementById('mUrl').value=r.url||'';
  document.getElementById('mAds').value=r.ads_url||'';
  document.getElementById('mPool').value=r.pool||'mahsa';
  document.getElementById('mMux').checked=!!r.use_mux;
  document.getElementById('mFrag').checked=!!r.use_fragment;
  document.getElementById('modalBg').classList.add('open');
}}
function closeModal(){{document.getElementById('modalBg').classList.remove('open')}}
async function saveModal(){{
  const id=document.getElementById('mId').value.trim();
  const body={{
    url:document.getElementById('mUrl').value.trim(),
    ads_url:document.getElementById('mAds').value.trim(),
    pool:document.getElementById('mPool').value,
    use_mux:document.getElementById('mMux').checked,
    use_fragment:document.getElementById('mFrag').checked,
  }};
  try{{
    if(id){{await api('/panel/api/configs/'+encodeURIComponent(id),{{method:'PATCH',body:JSON.stringify(body)}});toast('به‌روزرسانی شد')}}
    else{{await api('/panel/api/configs',{{method:'POST',body:JSON.stringify(body)}});toast('اضافه شد')}}
    closeModal();loadList();
  }}catch(e){{toast(String(e.message||e),true)}}
}}
async function delItem(id,name){{
  if(!id)return;
  const label=name||id;
  if(!window.confirm('حذف کانفیگ «'+label+'» از مهسا؟'))return;
  try{{await api('/panel/api/configs/'+encodeURIComponent(id),{{method:'DELETE'}});toast('حذف شد');loadList()}}
  catch(e){{toast(String(e.message||e),true)}}
}}
async function copyAllConfigs(){{
  const rows=window._lastResults||[];
  if(!rows.length){{toast('کانفیگی برای کپی نیست',true);return}}
  const nl=String.fromCharCode(10);
  const lines=rows.map(r=>{{
    const name=r.alias||r.id||'';
    const url=r.url||'';
    return name?('// '+name+nl+url):url;
  }}).filter(Boolean);
  const text=lines.join(nl+nl);
  try{{
    await navigator.clipboard.writeText(text);
    toast(rows.length+' کانفیگ کپی شد');
  }}catch(e){{
    toast('کپی ناموفق',true);
  }}
}}
loadList();
</script>
</body></html>
"""


# ─── Handlers ───────────────────────────────────────────────────────────────

async def handle_panel_root(request: web.Request):
    if not panel_is_enabled():
        return web.Response(text=DISABLED_HTML, content_type="text/html", charset="utf-8", status=503)
    if _valid_session(_token_from_request(request)):
        raise web.HTTPFound("/panel/")
    raise web.HTTPFound("/panel/login")


async def handle_login_get(request: web.Request):
    if not panel_is_enabled():
        return web.Response(text=DISABLED_HTML, content_type="text/html", charset="utf-8", status=503)
    if _valid_session(_token_from_request(request)):
        raise web.HTTPFound("/panel/")
    return web.Response(text=LOGIN_HTML, content_type="text/html", charset="utf-8")


async def _notify(ip: str, ok: bool, detail: str = ""):
    global _notify_login
    if not _notify_login:
        return
    try:
        res = _notify_login(ip, ok, detail)
        if hasattr(res, "__await__"):
            await res
    except Exception as e:
        log.warning("panel login notify failed: %s", e)


async def handle_login_post(request: web.Request):
    data = await request.post()
    pw = data.get("password", "")
    ip = _client_ip(request)
    if not check_password(pw):
        await _notify(ip, False, "wrong_or_expired_otp")
        html = LOGIN_HTML.replace(
            "رمز یک‌بارمصرف را از ربات تلگرام (دکمه درخواست رمز) دریافت کنید.",
            '<span style="color:var(--red-t)">رمز نادرست یا منقضی شده است. دوباره از ربات درخواست کنید.</span>',
        )
        return web.Response(text=html, content_type="text/html", charset="utf-8", status=401)
    await _notify(ip, True, "login_ok")
    resp = web.HTTPFound("/panel/")
    resp.set_cookie(
        "vira_panel_sess",
        _make_session_token(),
        max_age=_SESSION_TTL,
        httponly=True,
        samesite="Lax",
        path="/",
    )
    raise resp


async def handle_logout(request: web.Request):
    resp = web.HTTPFound("/panel/login")
    resp.del_cookie("vira_panel_sess", path="/")
    raise resp


@_require_auth
async def handle_dashboard(request: web.Request):
    if not panel_is_enabled():
        return web.Response(text=DISABLED_HTML, content_type="text/html", charset="utf-8", status=503)
    return web.Response(text=DASH_HTML, content_type="text/html", charset="utf-8")


@_require_auth
async def handle_api_list(request: web.Request):
    from mahsa_client import mahsa_list_configs

    q = request.rel_url.query
    is_active = None
    if q.get("is_active") == "true":
        is_active = True
    elif q.get("is_active") == "false":
        is_active = False
    try:
        limit = min(100, max(1, int(q.get("limit") or 30)))
        offset = max(0, int(q.get("offset") or 0))
    except Exception:
        limit, offset = 30, 0
    ok, data = await mahsa_list_configs(
        _mahsa_token(request),
        alias=q.get("alias") or None,
        hash=q.get("hash") or None,
        is_active=is_active,
        limit=limit,
        offset=offset,
    )
    if not ok:
        err = data.get("error") if isinstance(data, dict) else data
        return web.json_response({"ok": False, "error": err}, status=400)
    if isinstance(data, dict):
        return web.json_response({"ok": True, **data})
    return web.json_response({"ok": True, "results": data, "count": len(data) if isinstance(data, list) else 0})


@_require_auth
async def handle_api_create(request: web.Request):
    from mahsa_client import mahsa_create_config

    body = await request.json()
    ok, msg = await mahsa_create_config(
        url=body.get("url") or "",
        ads_url=body.get("ads_url") or "",
        token=_mahsa_token(request),
        pool=body.get("pool") or "mahsa",
        use_mux=bool(body.get("use_mux")),
        use_fragment=bool(body.get("use_fragment")),
    )
    if not ok:
        return web.json_response({"ok": False, "error": msg}, status=400)
    return web.json_response({"ok": True, "data": msg})


@_require_auth
async def handle_api_patch(request: web.Request):
    from mahsa_client import mahsa_update_config

    cid = request.match_info["id"]
    body = await request.json()
    ok, data = await mahsa_update_config(
        cid,
        _mahsa_token(request),
        url=body.get("url"),
        ads_url=body.get("ads_url"),
        pool=body.get("pool"),
        use_mux=body.get("use_mux"),
        use_fragment=body.get("use_fragment"),
        partial=True,
    )
    if not ok:
        err = data.get("error") if isinstance(data, dict) else data
        return web.json_response({"ok": False, "error": err}, status=400)
    return web.json_response({"ok": True, "data": data})


@_require_auth
async def handle_api_delete(request: web.Request):
    from mahsa_client import mahsa_delete_config

    cid = request.match_info["id"]
    ok, data = await mahsa_delete_config(cid, _mahsa_token(request))
    if not ok:
        err = data.get("error") if isinstance(data, dict) else data
        return web.json_response({"ok": False, "error": err}, status=400)
    return web.json_response({"ok": True})



async def handle_request_otp(request: web.Request):
    if not panel_is_enabled():
        return web.json_response({"ok": False, "error": "پنل غیرفعال است"}, status=503)
    ip = _client_ip(request)
    pw, ttl = request_password(60)
    if _notify_request:
        try:
            res = _notify_request(ip, pw, ttl)
            if hasattr(res, "__await__"):
                await res
        except Exception as e:
            log.warning("request-otp notify failed: %s", e)
            return web.json_response({"ok": False, "error": "ارسال به ربات ناموفق بود"}, status=500)
    else:
        log.warning("request-otp: no notify callback; password generated for IP %s", ip)
    return web.json_response({
        "ok": True,
        "message": f"رمز به ربات ادمین ارسال شد (اعتبار {ttl} ثانیه).",
        "ttl": ttl,
    })


def setup_panel_routes(app: web.Application, get_mahsa_token=None, on_login=None, on_request_otp=None, is_enabled=None):
    """Register panel routes. on_login(ip, ok, detail) optional async callback."""
    global _notify_login, _notify_request, _is_enabled
    if get_mahsa_token:
        app["get_mahsa_token"] = get_mahsa_token
    if on_login:
        _notify_login = on_login
    if on_request_otp:
        _notify_request = on_request_otp
    if is_enabled:
        _is_enabled = is_enabled
    app.router.add_get("/panel", handle_panel_root)
    app.router.add_get("/panel/", handle_dashboard)
    app.router.add_get("/panel/login", handle_login_get)
    app.router.add_post("/panel/login", handle_login_post)
    app.router.add_get("/panel/logout", handle_logout)
    app.router.add_post("/panel/api/request-otp", handle_request_otp)
    app.router.add_get("/panel/api/configs", handle_api_list)
    app.router.add_post("/panel/api/configs", handle_api_create)
    app.router.add_patch("/panel/api/configs/{id}", handle_api_patch)
    app.router.add_delete("/panel/api/configs/{id}", handle_api_delete)
    log.info("Mahsa panel routes ready at /panel/ (OTP %ss)", _OTP_TTL)
