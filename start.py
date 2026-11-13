#!/usr/bin/env python3
"""Phoenix / Daily Config entrypoint — Railway (Volume /data) + fallback hosts."""
import logging
import os
import runpy
import threading
import time
import json
import zipfile
import urllib.request
import shutil
import sqlite3
import subprocess


def _writable_dir():
    for d in [
        os.environ.get("DATA_DIR", ""),
        "/data",  # Railway Volume
        "/app/data",
        os.path.join(os.getcwd(), "data"),
        "/tmp/phoenix_data",
    ]:
        if not d:
            continue
        try:
            os.makedirs(d, exist_ok=True)
            test = os.path.join(d, ".w")
            with open(test, "w") as f:
                f.write("1")
            os.remove(test)
            return d
        except Exception:
            continue
    return os.getcwd()


def ensure_env_paths():
    data = _writable_dir()
    os.environ.setdefault("DATA_DIR", data)
    os.environ.setdefault("MPLCONFIGDIR", os.path.join(data, "mpl"))
    try:
        os.makedirs(os.environ["MPLCONFIGDIR"], exist_ok=True)
    except Exception:
        pass

    def fix(key, default_name):
        cur = os.environ.get(key, "")
        if not cur:
            os.environ[key] = os.path.join(data, default_name)
            return
        parent = os.path.dirname(cur) or "."
        if not os.path.isdir(parent):
            os.environ[key] = os.path.join(data, os.path.basename(cur) or default_name)

    fix("DB_FILE", "bot_data.db")
    fix("XRAY_BIN", "xray")
    fix("LOG_FILE", "bot.log")
    logging.info(
        "Paths: DATA_DIR=%s DB_FILE=%s XRAY_BIN=%s LOG_FILE=%s",
        os.environ.get("DATA_DIR"),
        os.environ.get("DB_FILE"),
        os.environ.get("XRAY_BIN"),
        os.environ.get("LOG_FILE"),
    )


def _disk_free_mb(path):
    try:
        st = os.statvfs(path)
        return (st.f_bavail * st.f_frsize) / (1024 * 1024)
    except Exception:
        return 9999


def ensure_xray():
    if os.environ.get("SKIP_XRAY", "").strip() in ("1", "true", "yes"):
        logging.info("SKIP_XRAY set — skipping xray download")
        return None
    target = os.environ.get("XRAY_BIN") or os.path.join(_writable_dir(), "xray")
    if os.path.isfile(target) and os.access(target, os.X_OK):
        logging.info("Xray OK: %s", target)
        return target
    free = _disk_free_mb(os.path.dirname(target) or ".")
    if free < 50:
        logging.error("Not enough disk for Xray (%.0f MB free) — set SKIP_XRAY=1 or free space", free)
        return None
    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    try:
        logging.info("Xray not found — downloading to %s ...", target)
        api = "https://api.github.com/repos/XTLS/Xray-core/releases/latest"
        req = urllib.request.Request(api, headers={"User-Agent": "phoenix-bot"})
        with urllib.request.urlopen(req, timeout=90) as r:
            data = json.loads(r.read().decode())
        url = None
        for asset in data.get("assets", []):
            name = asset.get("name", "")
            if name == "Xray-linux-64.zip" or (name.startswith("Xray-linux-64") and name.endswith(".zip")):
                url = asset.get("browser_download_url")
                break
        if not url:
            logging.error("Could not find Xray-linux-64.zip")
            return None
        zip_path = os.path.join(_writable_dir(), "xray_dl.zip")
        urllib.request.urlretrieve(url, zip_path)
        with zipfile.ZipFile(zip_path, "r") as z:
            member = next((n for n in z.namelist() if n == "xray" or n.endswith("/xray")), None)
            if not member:
                return None
            with z.open(member) as src, open(target, "wb") as dst:
                dst.write(src.read())
        os.chmod(target, 0o755)
        try:
            os.remove(zip_path)
        except Exception:
            pass
        logging.info("Xray installed: %s (%.1f MB)", target, os.path.getsize(target) / 1024 / 1024)
        return target
    except Exception as e:
        logging.error("Xray auto-install failed: %s", e)
        return None


def restore_db_from_url():
    url = (os.environ.get("DB_RESTORE_URL") or "").strip()
    if not url:
        return
    db_path = os.environ.get("DB_FILE")
    try:
        if os.path.isfile(db_path) and os.path.getsize(db_path) > 10_000:
            # still verify integrity
            if _integrity_ok(db_path):
                logging.info("DB exists and OK — skip restore")
                return
            logging.warning("DB exists but malformed — will re-download / recover")
    except Exception:
        pass
    free = _disk_free_mb(os.path.dirname(db_path) or ".")
    if free < 80:
        logging.error("Not enough disk to restore DB (%.0f MB free)", free)
        return
    try:
        logging.info("Restoring DB from DB_RESTORE_URL ...")
        tmp = os.path.join(_writable_dir(), "db_restore_download")
        urllib.request.urlretrieve(url, tmp)
        if zipfile.is_zipfile(tmp):
            with zipfile.ZipFile(tmp, "r") as z:
                member = next((n for n in z.namelist() if n.endswith(".db")), None)
                if not member:
                    logging.error("No .db in restore zip")
                    return
                with z.open(member) as src, open(db_path, "wb") as dst:
                    dst.write(src.read())
        else:
            shutil.move(tmp, db_path)
        logging.info("DB restored -> %s (%.1f KB)", db_path, os.path.getsize(db_path) / 1024)
    except Exception as e:
        logging.error("DB restore failed: %s", e)


def _integrity_ok(path):
    try:
        conn = sqlite3.connect(path, timeout=10)
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            return bool(row and row[0] == "ok")
        finally:
            conn.close()
    except Exception:
        return False


def recover_db_if_needed():
    """If bot_data.db is malformed after restore/crash, try sqlite .recover automatically."""
    db_path = os.environ.get("DB_FILE")
    if not db_path or not os.path.isfile(db_path):
        return
    if _integrity_ok(db_path):
        logging.info("DB integrity OK")
        return
    logging.warning("DB malformed — attempting automatic recover...")
    bak = db_path + f".malformed_{int(time.time())}"
    try:
        shutil.copy2(db_path, bak)
        logging.info("Malformed DB backed up to %s", bak)
    except Exception as e:
        logging.error("Could not backup malformed DB: %s", e)

    recovered = db_path + ".recovered"
    try:
        if os.path.exists(recovered):
            os.remove(recovered)
        # CLI recover
        r = subprocess.run(
            ["sqlite3", db_path, ".recover"],
            capture_output=True, text=True, timeout=180,
        )
        if r.returncode == 0 and r.stdout and len(r.stdout) > 100:
            conn = sqlite3.connect(recovered)
            try:
                conn.executescript(r.stdout)
                conn.commit()
            finally:
                conn.close()
            if _integrity_ok(recovered):
                shutil.move(recovered, db_path)
                logging.info("✅ DB recovered successfully via sqlite3 .recover")
                return
    except FileNotFoundError:
        logging.warning("sqlite3 CLI not in image — trying dump fallback")
    except Exception as e:
        logging.error("recover failed: %s", e)

    # Last resort: if still bad, rename away so bot creates fresh schema
    if not _integrity_ok(db_path):
        try:
            dead = db_path + f".dead_{int(time.time())}"
            shutil.move(db_path, dead)
            logging.error(
                "DB unrecoverable — moved to %s. Bot will create empty DB. "
                "Restore a healthy backup into DB_FILE when available.",
                dead,
            )
        except Exception as e:
            logging.error("Could not quarantine DB: %s", e)


def run_user_bot():
    token = os.environ.get("USER_BOT_TOKEN") or os.environ.get("BOT_TOKEN_3")
    if not token:
        logging.info("USER_BOT_TOKEN not set - skipping Vault user bot")
        return
    # log under DATA_DIR to avoid read-only workspace paths
    data = os.environ.get("DATA_DIR") or _writable_dir()
    try:
        os.chdir(os.environ.get("PWD") or os.getcwd())
    except Exception:
        pass
    while True:
        try:
            logging.info("Starting Vault user bot...")
            runpy.run_path("user_bot.py", run_name="__main__")
            logging.warning("Vault user bot exited cleanly; restarting in 5s")
        except Exception as e:
            logging.error("Vault user bot failed: %s — restart in 15s", e)
            time.sleep(15)
            continue
        time.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    ensure_env_paths()
    restore_db_from_url()
    recover_db_if_needed()
    ensure_xray()
    token = os.environ.get("USER_BOT_TOKEN") or os.environ.get("BOT_TOKEN_3")
    if token:
        t = threading.Thread(target=run_user_bot, name="user-bot", daemon=True)
        t.start()
        logging.info("Started Vault user bot in background thread (auto-restart)")

    runpy.run_path("bot.py", run_name="__main__")
