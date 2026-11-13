# -*- coding: utf-8 -*-
"""
Config Vault — User Bot v1.2
Professional companion bot for Railway.
"""
from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime
from typing import List, Tuple

from telethon import TelegramClient, events, Button
try:
    from telethon.tl.types import KeyboardButtonCopy  # type: ignore
except ImportError:
    KeyboardButtonCopy = None  # older Telethon builds
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.errors import UserNotParticipantError, MessageNotModifiedError

import vault_core as core
from vault_core import (
    get_setting, set_setting, db_init, count_pool, fetch_pool,
    top_countries, top_protocols, flag, check_rate, mark_rate,
    progress_text, test_batch, build_txt, XRAY_OK, xray_available, pool_hours,
    annotate_links_nimbaha,

)

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", "0"))
USER_BOT_TOKEN = os.environ.get("USER_BOT_TOKEN") or os.environ.get("BOT_TOKEN_3", "")
VERSION = "1.2.0"

XRAY_DISCLAIMER = (
    "⚠️ **نکته مهم در مورد تست:**\n\n"
    "تست کانفیگ‌های اخیر توسط هسته قدرتمند Xray و روی سرورهای خارج از ایران انجام می‌گیرد.\n\n"
    "لذا ممکن است یک کانفیگ در سرورِ تست کاملاً سالم باشد، اما روی اینترنت شما (ISP) محدود یا فیلتر شده باشد."
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("user_bot.log", "a", "utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("user_bot")

_copy_cache = {}

bot = TelegramClient("user_bot.session", API_ID, API_HASH)

PROTO_LABEL = {
    "vless": "VLESS",
    "vmess": "VMess",
    "trojan": "Trojan",
    "ss": "Shadowsocks",
    "hysteria2": "Hysteria2",
    "telegram proxy": "TG Proxy",
}


COUNTRY_FA = {
    "IR": "ایران", "DE": "آلمان", "NL": "هلند", "US": "آمریکا", "GB": "انگلیس", "UK": "انگلیس",
    "FR": "فرانسه", "TR": "ترکیه", "AE": "امارات", "CA": "کانادا", "AU": "استرالیا",
    "FI": "فنلاند", "SE": "سوئد", "NO": "نروژ", "DK": "دانمارک", "PL": "لهستان",
    "IT": "ایتالیا", "ES": "اسپانیا", "RU": "روسیه", "UA": "اوکراین", "JP": "ژاپن",
    "KR": "کره جنوبی", "SG": "سنگاپور", "HK": "هنگ‌کنگ", "IN": "هند", "CN": "چین",
    "BR": "برزیل", "AR": "آرژانتین", "MX": "مکزیک", "CH": "سوئیس", "AT": "اتریش",
    "BE": "بلژیک", "CZ": "چک", "RO": "رومانی", "BG": "بلغارستان", "GR": "یونان",
    "PT": "پرتغال", "IE": "ایرلند", "IL": "اسرائیل", "IQ": "عراق", "AF": "افغانستان",
    "PK": "پاکستان", "ID": "اندونزی", "MY": "مالزی", "TH": "تایلند", "VN": "ویتنام",
    "PH": "فیلیپین", "TW": "تایوان", "NZ": "نیوزیلند", "ZA": "آفریقای جنوبی",
    "EG": "مصر", "SA": "عربستان", "KZ": "قزاقستان", "UZ": "ازبکستان", "AM": "ارمنستان",
    "AZ": "آذربایجان", "GE": "گرجستان", "LT": "لیتوانی", "LV": "لاتویا", "EE": "استونی",
    "HU": "مجارستان", "SK": "اسلواکی", "SI": "اسلوونی", "HR": "کرواسی", "RS": "صربستان",
    "MD": "مولداوی", "BY": "بلاروس", "LU": "لوکزامبورگ", "IS": "ایسلند", "MT": "مالت",
    "CY": "قبرس", "BA": "بوسنی", "MK": "مقدونیه", "AL": "آلبانی", "ME": "مونته‌نگرو",
    "OM": "عمان", "QA": "قطر", "KW": "کویت", "BH": "بحرین", "JO": "اردن",
    "LB": "لبنان", "SY": "سوریه", "YE": "یمن", "TM": "ترکمنستان", "TJ": "تاجیکستان",
    "KG": "قرقیزستان", "MN": "مغولستان", "NP": "نپال", "LK": "سری‌لانکا", "BD": "بنگلادش",
    "NG": "نیجریه", "KE": "کنیا", "MA": "مراکش", "TN": "تونس", "DZ": "الجزایر",
    "CL": "شیلی", "CO": "کلمبیا", "PE": "پرو", "VE": "ونزوئلا", "UY": "اروگوئه",
    "PA": "پاناما", "CR": "کاستاریکا", "CU": "کوبا", "PR": "پورتوریکو",
    "XX": "نامشخص",
}

# English / alternate names for search (lowercase)
COUNTRY_ALIASES = {
    "oman": "OM", "عمان": "OM", "germany": "DE", "آلمان": "DE", "netherlands": "NL", "holland": "NL", "هلند": "NL",
    "usa": "US", "america": "US", "آمریکا": "US", "uk": "GB", "england": "GB", "britain": "GB", "انگلیس": "GB",
    "france": "FR", "فرانسه": "FR", "turkey": "TR", "turkiye": "TR", "ترکیه": "TR",
    "uae": "AE", "emirates": "AE", "امارات": "AE", "canada": "CA", "کانادا": "CA",
    "australia": "AU", "استرالیا": "AU", "finland": "FI", "فنلاند": "FI",
    "sweden": "SE", "سوئد": "SE", "norway": "NO", "نروژ": "NO", "denmark": "DK", "دانمارک": "DK",
    "poland": "PL", "لهستان": "PL", "italy": "IT", "ایتالیا": "IT", "spain": "ES", "اسپانیا": "ES",
    "russia": "RU", "روسیه": "RU", "ukraine": "UA", "اوکراین": "UA", "japan": "JP", "ژاپن": "JP",
    "korea": "KR", "southkorea": "KR", "singapore": "SG", "سنگاپور": "SG",
    "hongkong": "HK", "هنگکنگ": "HK", "india": "IN", "هند": "IN", "china": "CN", "چین": "CN",
    "brazil": "BR", "برزیل": "BR", "switzerland": "CH", "سوئیس": "CH", "austria": "AT", "اتریش": "AT",
    "belgium": "BE", "بلژیک": "BE", "romania": "RO", "رومانی": "RO", "greece": "GR", "یونان": "GR",
    "portugal": "PT", "ireland": "IE", "israel": "IL", "iraq": "IQ", "عراق": "IQ",
    "afghanistan": "AF", "افغانستان": "AF", "pakistan": "PK", "پاکستان": "PK",
    "indonesia": "ID", "malaysia": "MY", "thailand": "TH", "vietnam": "VN",
    "philippines": "PH", "taiwan": "TW", "egypt": "EG", "مصر": "EG", "saudi": "SA", "عربستان": "SA",
    "kazakhstan": "KZ", "uzbekistan": "UZ", "armenia": "AM", "azerbaijan": "AZ", "georgia": "GE",
    "lithuania": "LT", "latvia": "LV", "estonia": "EE", "hungary": "HU", "croatia": "HR",
    "serbia": "RS", "qatar": "QA", "kuwait": "KW", "bahrain": "BH", "jordan": "JO",
    "lebanon": "LB", "syria": "SY", "yemen": "YE", "iran": "IR", "ایران": "IR",
}


def country_title(cc: str) -> str:
    cc = (cc or "").upper().strip()
    name = COUNTRY_FA.get(cc, cc)
    return "{} {} ({})".format(flag(cc), name, cc)



def parse_channels(raw: str) -> List[str]:
    if not raw:
        return []
    # پشتیبانی از جداکننده کاما، خط جدید یا اسپیس
    cleaned = raw.replace("\n", ",").replace(" ", ",")
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    return parts


async def is_member(user_id: int, channel_ref: str) -> bool:
    if not channel_ref:
        return True
    try:
        entity = await bot.get_entity(channel_ref)
        await bot(GetParticipantRequest(entity, user_id))
        return True
    except UserNotParticipantError:
        return False
    except Exception as e:
        log.warning(f"join-check for {channel_ref}: {e}")
        return True


async def check_all_channels(user_id: int, channels: List[str]) -> Tuple[bool, List[str]]:
    """چک کردن عضویت کاربر در تمام کانال‌ها. خروجی: (عضو همه هست یا نه, لیست کانال‌هایی که عضو نیست)"""
    missing = []
    for ch in channels:
        if not await is_member(user_id, ch):
            missing.append(ch)
    return len(missing) == 0, missing


def ch_link(ref: str) -> str:
    if not ref:
        return ""
    if ref.startswith("http"):
        return ref
    if ref.startswith("@"):
        return f"https://t.me/{ref[1:]}"
    return f"https://t.me/{ref.lstrip('@')}"


async def ensure_access(event) -> bool:
    uid = event.sender_id
    if uid == ADMIN_USER_ID:
        return True
    if get_setting("userbot_enabled", "1") != "1":
        msg = "🔴 ربات موقتاً خاموش است. لطفاً بعداً سر بزنید."
        try:
            if hasattr(event, "answer"):
                await event.answer("ربات موقتا خاموش است", alert=True)
            else:
                await event.respond(msg, parse_mode="md")
        except Exception:
            pass
        return False
    if get_setting("userbot_force_join", "0") == "1":
        raw_ch = get_setting("userbot_lock_channel", "")
        channels = parse_channels(raw_ch)
        if channels:
            is_ok, missing = await check_all_channels(uid, channels)
            if not is_ok:
                btns = []
                for idx, ch in enumerate(missing, 1):
                    label = f"📢 عضویت در کانال {idx}" if len(missing) > 1 else "📢 عضویت در کانال"
                    btns.append([Button.url(label, ch_link(ch))])
                btns.append([Button.inline("✅ عضو شدم — ادامه", "check_join")])
                
                text = (
                    "🔒 **دسترسی محدود**\n\n"
                    "برای استفاده از ربات و دریافت کانفیگ‌ها باید در کانال‌های زیر عضو شوید.\n"
                    "بعد از عضویت روی دکمه **عضو شدم — ادامه** کلیک کنید."
                )
                try:
                    if hasattr(event, "data"):
                        await event.edit(text, buttons=btns, parse_mode="md")
                    else:
                        await event.respond(text, buttons=btns, parse_mode="md")
                except Exception:
                    await event.respond(text, buttons=btns, parse_mode="md")
                return False
    return True


def home_text() -> str:
    hours = get_setting("userbot_pool_hours", "1")
    total = count_pool()
    if total == 0:
        return (
            "💎 **دریافت کانفیگ روزانه**\n\n"
            "استخر فعلا خالی است.\n"
            "چند دقیقه دیگر دوباره امتحان کنید."
        )
    return (
        "💎 **دریافت کانفیگ روزانه**\n\n"
        "بهترین و تازه‌ترین کانفیگ‌های جمع‌آوری‌شده در اختیار شماست.\n"
        "شما می‌توانید بر اساس کشور یا پروتکل فیلتر مورد نظر خود را اعمال کنید.\n\n"
        f"⏱️ بازه نگهداری: `{hours}` ساعت اخیر\n"
        f"📦 موجودی فعال: `{total}` کانفیگ\n\n"
        "👇 لطفاً یکی از گزینه‌های زیر را انتخاب کنید:"
    )

def home_buttons(is_admin=False):
    total = count_pool()
    max_n = int(get_setting("userbot_max_per_request", "40") or 40)
    show = min(total, max_n) if total else 0
    btns = [
        [Button.inline(f"🧪 تست کانفیگ‌های اخیر ( {show} )", "go_xray_ALL_ALL")],
        [Button.inline(f"⚡️ دریافت سریع ( {show} )", "go_fast_ALL_ALL")],
        [
            Button.inline("🌍 کشورها", "menu_countries"),
            Button.inline("⚙️ پروتکل‌ها", "menu_protos"),
        ],
        [Button.inline("📊 آمار استخر", "pool_stats")],
    ]
    if is_admin:
        btns.append([Button.inline("🛠 پنل مدیریت", "admin")])
    return btns


def countries_menu():
    rows = top_countries()
    if not rows:
        return "هنوز کشوری ثبت نشده.", [
            [Button.inline("🔎 جستجوی کشور", "search_country")],
            [Button.inline("◀️ بازگشت", "home")],
        ]
    text = "🌍 انتخاب کشور" + chr(10) + chr(10) + "یکی را انتخاب کنید یا کشور دلخواه را جستجو کنید:"
    btns, line = [], []
    for cc, c in rows:
        name = COUNTRY_FA.get((cc or "").upper(), cc)
        line.append(Button.inline("{} {} · {}".format(flag(cc), name, c), "pick_country_{}".format(cc)))
        if len(line) == 2:
            btns.append(line)
            line = []
    if line:
        btns.append(line)
    btns.append([Button.inline("🔎 جستجوی کشور", "search_country")])
    btns.append([Button.inline("◀️ بازگشت", "home")])
    return text, btns



def protos_menu():
    rows = top_protocols()
    if not rows:
        return "هنوز پروتکلی ثبت نشده.", [[Button.inline("◀️ بازگشت", "home")]]
    text = "⚙️ **انتخاب پروتکل**\n\nیکی را انتخاب کنید:"
    btns, line = [], []
    for tname, c in rows:
        label = PROTO_LABEL.get(tname, tname)
        safe = tname.replace("_", "¦")
        line.append(Button.inline(f"{label}  ·  {c}", f"pick_proto_{safe}"))
        if len(line) == 2:
            btns.append(line)
            line = []
    if line:
        btns.append(line)
    btns.append([Button.inline("◀️ بازگشت", "home")])
    return text, btns


def pick_actions(kind, value, count):
    if kind == "country":
        title = country_title(value)
        xray_cb = "go_xray_{}_ALL".format(value)
        fast_cb = "go_fast_{}_ALL".format(value)
        back = "menu_countries"
    else:
        title = PROTO_LABEL.get(value, value)
        safe = value.replace("_", "¦")
        xray_cb = "go_xray_ALL_{}".format(safe)
        fast_cb = "go_fast_ALL_{}".format(safe)
        back = "menu_protos"
    if count <= 0:
        text = (
            "🏷 " + title + chr(10) + chr(10)
            + "فعلا کانفیگی برای این فیلتر نیست." + chr(10)
            + "چند دقیقه دیگر دوباره سر بزنید."
        )
        return text, [[Button.inline("◀️ بازگشت", back)]]

    max_req = min(count, int(get_setting("userbot_max_per_request", "40") or 40))
    text = (
        "🏷 " + title + chr(10) + chr(10)
        + "📦 موجودی: {} کانفیگ".format(count) + chr(10) + chr(10)
        + "👇 روش دریافت را انتخاب کنید:"
    )
    btns = [
        [Button.inline("🧪 تست کانفیگ‌های اخیر ( {} )".format(max_req), xray_cb)],
        [Button.inline("⚡️ دریافت سریع ( {} )".format(max_req), fast_cb)],
        [Button.inline("◀️ بازگشت", back)],
    ]
    return text, btns



def admin_text():
    raw_ch = get_setting('userbot_lock_channel','') or '—'
    channels = parse_channels(raw_ch)
    ch_display = ", ".join(channels) if channels else "—"
    
    return (
        f"🛠 **پنل مدیریت Vault** `v{VERSION}`\n\n"
        f"وضعیت ربات: {'🟢 روشن' if get_setting('userbot_enabled','1')=='1' else '🔴 خاموش'}\n"
        f"قفل کانال: {'🟢 فعال' if get_setting('userbot_force_join','0')=='1' else '🔴 خاموش'}\n"
        f"تعداد کانال‌های قفل: `{len(channels)}` (`{ch_display}`)\n"
        f"بازه استخر: `{get_setting('userbot_pool_hours','1')}` ساعت\n"
        f"محدودیت کاربر: هر `{get_setting('userbot_rate_minutes','15')}` دقیقه\n"
        f"سقف هر درخواست: `{get_setting('userbot_max_per_request','40')}`\n"
        f"موجودی الان: **{count_pool()}**\n"
        f"Xray: {'✅' if (XRAY_OK and xray_available()) else '❌'}"
    )


def admin_buttons():
    en = get_setting("userbot_enabled", "1") == "1"
    fj = get_setting("userbot_force_join", "0") == "1"
    return [
        [Button.inline("🔴 خاموش کردن" if en else "🟢 روشن کردن", "adm_bot")],
        [Button.inline("🔓 حذف قفل" if fj else "🔒 فعال‌سازی قفل کانال", "adm_lock")],
        [Button.inline("📢 تنظیم کانال‌ها (چندتا با کاما)", "adm_channel")],
        [
            Button.inline("⏱ بازه استخر", "adm_hours"),
            Button.inline("⏳ محدودیت", "adm_rate"),
        ],
        [Button.inline("🔢 سقف درخواست", "adm_max")],
        [Button.inline("🔙 منوی کاربر", "home")],
    ]


async def deliver(event, country: str, proto: str, use_xray: bool):
    uid = event.sender_id

    if uid != ADMIN_USER_ID:
        ok, remain = check_rate(uid)
        if not ok:
            m, s = divmod(remain, 60)
            return await event.answer(f"صبر کنید: {m}m {s}s", alert=True)

    limit = int(get_setting("userbot_max_per_request", "40") or 40)
    configs = fetch_pool(
        country=None if country == "ALL" else country,
        proto=None if proto == "ALL" else proto,
        limit=limit,
    )
    if not configs:
        return await event.answer("برای این فیلتر کانفیگی نیست", alert=True)

    mark_rate(uid)
    await event.answer()

    mode_label = "Xray Live Test" if use_xray else "Quick Export"
    extra = f"\n\n{XRAY_DISCLAIMER}" if use_xray else ""
    status_msg = await event.respond(
        f"⏳ **در حال آماده‌سازی**\n\n"
        f"تعداد: `{len(configs)}`\n"
        f"حالت: `{mode_label}`\n"
        f"فیلتر: `{country}` / `{proto}`"
        f"{extra}",
        parse_mode="md",
    )

    if use_xray and XRAY_OK and xray_available():
        async def on_prog(done, total, on, off):
            try:
                await bot.edit_message(
                    status_msg,
                    progress_text(done, total, on, off, title="تست کانفیگ‌های اخیر"),
                    parse_mode="md",
                )
            except MessageNotModifiedError:
                pass
            except Exception:
                pass

        online_rows, offline = await test_batch(
            configs, use_xray=True, on_progress=on_prog, parallel=2
        )
        links = [r["link"] for r in online_rows]
        meta = {
            "mode": "xray",
            "country": country,
            "proto": proto,
            "total": len(configs),
            "online": len(links),
            "offline": offline,
        }
        caption = (
            f"✅ **پکیج کانفیگ‌های شما آماده شد**\n\n"
            f"🔍 **وضعیت تست:**\n"
            f"بررسی شده: {len(configs)}\n"
            f"🟢 متصل و سالم: {len(links)}\n"
            f"🔴 غیرفعال: {offline}\n\n"
            f"🏷 **جزئیات فیلتر:** {country} · {proto}\n"
            f"📁 **فرمت خروجی:** متنی (TXT)\n\n"
            f"{XRAY_DISCLAIMER}"
        )
    else:
        links = [c["link"] for c in configs]
        meta = {
            "mode": "fast",
            "country": country,
            "proto": proto,
            "total": len(links),
            "online": len(links),
            "offline": 0,
        }
        caption = (
            f"⚡️ **خروجی سریع**\n\n"
            f"📦 تعداد: `{len(links)}`\n"
            f"🏷 فیلتر: `{country}` · `{proto}`\n"
            f"ℹ️ بدون تست مجدد (از استخر اصلی)\n"
            f"📁 فرمت: `TXT`"
        )

    if not links:
        empty = (
            "❌ در این فیلتر کانفیگ آنلاینی پیدا نشد." + chr(10) + "کشور یا پروتکل دیگری را امتحان کنید."
        )
        try:
            await bot.edit_message(status_msg, empty)
        except Exception:
            await event.respond(empty)
        return

    if len(links) < 5:
        # Message 1: summary info | Message 2: reply with mono configs
        mode_fa = "تست Xray" if use_xray else "دریافت سریع"
        info = (
            f"✅ آماده شد" + chr(10) + chr(10)
            + f"📦 تعداد: {len(links)}" + chr(10)
            + f"🌍 فیلتر: {country}" + chr(10)
            + f"⚙️ پروتکل: {proto}" + chr(10)
            + f"🛠 حالت: {mode_fa}"
        )
        try:
            await bot.edit_message(status_msg, info, parse_mode="md")
            info_msg = status_msg
        except Exception:
            info_msg = await event.respond(info, parse_mode="md")

        try:
            parts = await annotate_links_nimbaha(links)
        except Exception:
            parts = [f"`{link}`" for link in links]
        body = chr(10).join(parts)
        await bot.send_message(
            event.chat_id,
            body,
            parse_mode="md",
            reply_to=info_msg.id,
        )

    else:
        content = build_txt(links, meta)
        fname = f"configs_{country}_{proto.replace(' ', '')}_{datetime.now().strftime('%H%M%S')}.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(content)
        await bot.send_file(event.chat_id, fname, caption=caption, parse_mode="md")
        try:
            os.remove(fname)
        except Exception:
            pass
        try:
            await bot.edit_message(
                status_msg,
                f"✅ آماده شد" + chr(10) + chr(10) + f"📦 تعداد: {len(links)}" + chr(10) + f"🌍 فیلتر: {country}" + chr(10) + f"⚙️ پروتکل: {proto}" + chr(10) + "📁 فایل TXT",
                parse_mode="md",
            )
        except Exception:
            pass


@bot.on(events.NewMessage(pattern=r"^/start"))
async def on_start(event):
    if not await ensure_access(event):
        return
    uid = event.sender_id
    first_key = f"userbot_welcomed_{uid}"
    if get_setting(first_key, "0") != "1":
        guide = (
            "📖 راهنمای دکمه‌ها\n\n"
            "🧪 تست کانفیگ‌های اخیر\n"
            "کانفیگ‌ها با هسته Xray تست می‌شوند و فقط سالم‌ها ارسال می‌شوند.\n\n"
            "⚡️ دریافت سریع\n"
            "بدون تست دوباره، از استخر یک‌ساعته فایل می‌گیرید.\n\n"
            "🌍 کشورها / ⚙️ پروتکل‌ها\n"
            "فیلتر بر اساس کشور یا نوع کانفیگ.\n\n"
            "📊 آمار استخر\n"
            "وضعیت موجودی فعلی.\n\n"
            f"{XRAY_DISCLAIMER}"
        )
        try:
            msg = await event.respond(guide)
            try:
                await bot.pin_message(event.chat_id, msg.id, notify=False)
            except Exception:
                pass
            set_setting(first_key, "1")
        except Exception as e:
            log.warning("first-start guide failed: %s", e)
    await event.respond(
        home_text(),
        buttons=home_buttons(uid == ADMIN_USER_ID),
        parse_mode="md"
    )


@bot.on(events.NewMessage(pattern=r"^/admin"))
async def on_admin(event):
    if event.sender_id != ADMIN_USER_ID:
        return
    await event.respond(admin_text(), buttons=admin_buttons(), parse_mode="md")


@bot.on(events.CallbackQuery)
async def on_cb(event):
    data = event.data.decode("utf-8")
    uid = event.sender_id

    if data == "admin" or data.startswith("adm_"):
        if uid != ADMIN_USER_ID:
            return await event.answer("فقط ادمین", alert=True)
        if data == "admin":
            return await event.edit(admin_text(), buttons=admin_buttons(), parse_mode="md")
        if data == "adm_bot":
            set_setting(
                "userbot_enabled",
                "0" if get_setting("userbot_enabled", "1") == "1" else "1",
            )
            return await event.edit(admin_text(), buttons=admin_buttons(), parse_mode="md")
        if data == "adm_lock":
            set_setting(
                "userbot_force_join",
                "0" if get_setting("userbot_force_join", "0") == "1" else "1",
            )
            return await event.edit(admin_text(), buttons=admin_buttons(), parse_mode="md")

        prompts = {
            "adm_channel": ("یوزرنیم یا لینک کانال‌ها را بفرستید (بین کانال‌ها کاما `,` بگذارید):\nمثال: `@ch1, @ch2, @ch3`", "userbot_lock_channel", None),
            "adm_hours": ("بازه استخر به **ساعت** (عدد):\nمثال: `1`", "userbot_pool_hours", "posint"),
            "adm_rate": ("فاصله مجاز هر کاربر به **دقیقه**:\nمثال: `15` — صفر = بدون محدودیت", "userbot_rate_minutes", "int"),
            "adm_max": ("حداکثر کانفیگ در هر درخواست:\nمثال: `40`", "userbot_max_per_request", "posint"),
        }
        if data in prompts:
            prompt, key, kind = prompts[data]
            async with bot.conversation(uid, timeout=90) as conv:
                await conv.send_message(prompt, parse_mode="md")
                try:
                    resp = await conv.get_response()
                except asyncio.TimeoutError:
                    return await conv.send_message("زمان تمام شد.")
                val = (resp.text or "").strip()
                if kind == "posint" and (not val.isdigit() or int(val) <= 0):
                    return await conv.send_message("❌ عدد معتبر وارد کنید.")
                if kind == "int" and not val.isdigit():
                    return await conv.send_message("❌ عدد معتبر وارد کنید.")
                set_setting(key, val)
                await conv.send_message(f"✅ ذخیره شد: `{val}`", parse_mode="md")
            return await event.respond(admin_text(), buttons=admin_buttons(), parse_mode="md")

    if not await ensure_access(event):
        return

    if data == "check_join":
        raw_ch = get_setting("userbot_lock_channel", "")
        channels = parse_channels(raw_ch)
        is_ok, missing = await check_all_channels(uid, channels)
        if is_ok:
            await event.edit(home_text(), buttons=home_buttons(uid == ADMIN_USER_ID), parse_mode="md")
        else:
            await event.answer(f"هنوز در {len(missing)} کانال عضو نشده‌اید!", alert=True)
        return

    if data == "home":
        return await event.edit(home_text(), buttons=home_buttons(uid == ADMIN_USER_ID), parse_mode="md")

    if data == "help":
        text = (
            "ℹ️ **راهنمای ربات**\n\n"
            f"• استخر: کانفیگ‌های **{get_setting('userbot_pool_hours','1')} ساعت** اخیر\n"
            "• **تست Xray**: بررسی واقعی آنلاین بودن (روی سرور خارج از ایران)\n"
            "• ممکن است کانفیگ اینجا آنلاین باشد ولی در ایران فیلتر باشد\n"
            "• **دریافت سریع**: بدون تست مجدد\n"
            f"• محدودیت: هر **{get_setting('userbot_rate_minutes','15')} دقیقه** یک‌بار\n"
            "• خروجی: فایل `.txt` آماده وارد کردن\n\n"
            "اگر استخر خالی است، ربات اصلی هنوز داده نساخته."
        )
        return await event.edit(text, buttons=[[Button.inline("🔙 بازگشت", "home")]], parse_mode="md")

    if data == "pool_stats":
        hours = get_setting("userbot_pool_hours", "1")
        total = count_pool()
        lines = [
            f"کل کانفیگ‌ها: **{total}**\n",
        ]
        countries = top_countries(8)
        if countries:
            lines.append("**کشورها:**")
            for cc, c in countries:
                lines.append("• {} {} — {}".format(flag(cc), COUNTRY_FA.get((cc or "").upper(), cc), c))
        protos = top_protocols()
        if protos:
            lines.append("\n**پروتکل‌ها:**")
            for t, c in protos:
                lines.append(f"• `{PROTO_LABEL.get(t, t)}` — {c}")
        if total == 0:
            lines.append("\n📭 استخر خالی است.")
        return await event.edit(
            "\n".join(lines),
            buttons=[[Button.inline("🔙 بازگشت", "home")]],
            parse_mode="md",
        )

    if data == "menu_countries":
        text, btns = countries_menu()
        return await event.edit(text, buttons=btns, parse_mode="md")

    if data == "menu_protos":
        text, btns = protos_menu()
        return await event.edit(text, buttons=btns, parse_mode="md")


    if data.startswith("copycfg_"):
        key = data.split("copycfg_", 1)[1]
        links = _copy_cache.get(key)
        if not links:
            return await event.answer("منقضی شده — دوباره دریافت کنید", alert=True)
        await event.answer("متن برای کپی ارسال شد")
        block = "```" + chr(10) + chr(10).join(links) + chr(10) + "```"
        await event.respond(
            "📋 برای کپی: لمس طولانی روی متن زیر" + chr(10) + chr(10) + block,
            parse_mode="md",
        )
        return

    if data == "search_country":

        await event.answer()
        async with bot.conversation(uid, timeout=60) as conv:
            await conv.send_message(
                "🔎 کد یا نام کشور را بفرستید." + chr(10)
                + "مثال: IR یا ایران یا de"
            )
            try:
                resp = await conv.get_response()
            except asyncio.TimeoutError:
                return await conv.send_message("زمان تمام شد.")
            raw = (resp.text or "").strip()
            if not raw:
                return await conv.send_message("ورودی خالی بود.")
            q = raw.upper().replace(" ", "")
            q_low = raw.lower().replace(" ", "").replace("‌", "")
            resolved = None
            if len(q) == 2 and q.isalpha():
                resolved = "GB" if q == "UK" else q
            if not resolved and q_low in COUNTRY_ALIASES:
                resolved = COUNTRY_ALIASES[q_low]
            if not resolved:
                for code, fa in COUNTRY_FA.items():
                    if raw == fa or fa in raw or raw in fa or q == code:
                        resolved = code
                        break
            if not resolved:
                for alias, code in COUNTRY_ALIASES.items():
                    if q_low in alias or alias in q_low:
                        resolved = code
                        break
            if not resolved:
                return await conv.send_message(
                    "کشور «{}» پیدا نشد.".format(raw) + chr(10)
                    + "مثال: OM یا عمان یا Oman"
                )
            n = count_pool(country=resolved)
            if n <= 0:
                return await conv.send_message(
                    country_title(resolved) + chr(10) + chr(10)
                    + "در استخر فعلی کانفیگی از این کشور نیست."
                )
            text, btns = pick_actions("country", resolved, n)
            await conv.send_message(text, buttons=btns)
        return

    if data.startswith("pick_country_"):
        cc = data.split("pick_country_", 1)[1]
        text, btns = pick_actions("country", cc, count_pool(country=cc))
        return await event.edit(text, buttons=btns, parse_mode="md")

    if data.startswith("pick_proto_"):
        proto = data.split("pick_proto_", 1)[1].replace("¦", "_")
        text, btns = pick_actions("proto", proto, count_pool(proto=proto))
        return await event.edit(text, buttons=btns, parse_mode="md")

    if data.startswith("go_xray_") or data.startswith("go_fast_"):
        use_xray = data.startswith("go_xray_")
        payload = data.split("_", 2)[2]
        if "_" not in payload:
            return await event.answer("درخواست نامعتبر", alert=True)
        country, proto = payload.split("_", 1)
        proto = proto.replace("¦", "_")
        if use_xray and not (XRAY_OK and xray_available()):
            await event.answer("Xray نیست — خروجی سریع", alert=True)
            use_xray = False
        await deliver(event, country, proto, use_xray)
        return


async def main():
    if not USER_BOT_TOKEN:
        log.critical("USER_BOT_TOKEN / BOT_TOKEN_3 is not set")
        print("Set USER_BOT_TOKEN environment variable")
        return
    if not API_ID or not API_HASH:
        log.critical("API_ID / API_HASH missing")
        return

    db_init()
    if get_setting("userbot_pool_hours", "1") == "5":
        set_setting("userbot_pool_hours", "1")
    await bot.start(bot_token=USER_BOT_TOKEN)
    me = await bot.get_me()
    log.info(f"Vault bot @{me.username} | pool={count_pool()} | xray={XRAY_OK and xray_available()}")

    try:
        await bot.send_message(
            ADMIN_USER_ID,
            f"🚀 **ربات** `v{VERSION}` آنلاین شد\n"
            f"📦 استخر: `{count_pool()}` کانفیگ\n"
            f"🧪 Xray: {'✅' if (XRAY_OK and xray_available()) else '❌'}\n"
            f"⏱ بازه: `{get_setting('userbot_pool_hours','1')}`h\n\n"
            f"دستور ادمین: /admin",
            parse_mode="md",
        )
    except Exception as e:
        log.warning(f"admin notify: {e}")

    await bot.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("stopped")