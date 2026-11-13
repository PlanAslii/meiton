# -*- coding: utf-8 -*-
"""Start main Phoenix bot and optional Vault user bot together (Railway-friendly)."""
import logging
import os
import runpy
import threading
import time


def _resolve_user_bot_token() -> str:
    token = (os.environ.get("USER_BOT_TOKEN") or os.environ.get("BOT_TOKEN_3") or "").strip()
    if token:
        return token
    try:
        import userbot_store
        userbot_store.init()
        return (userbot_store.get_token() or "").strip()
    except Exception as e:
        logging.warning("userbot_store token read failed: %s", e)
        return ""


def run_user_bot():
    token = _resolve_user_bot_token()
    if not token:
        logging.info("USER_BOT_TOKEN / userbot_meta not set - skipping Vault user bot")
        return
    # Ensure child process/path sees the token
    os.environ["USER_BOT_TOKEN"] = token
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
    token = _resolve_user_bot_token()
    if token:
        t = threading.Thread(target=run_user_bot, name="user-bot", daemon=True)
        t.start()
        logging.info("Started Vault user bot in background thread (auto-restart)")
    else:
        logging.info("Vault user bot not started (no token in env or userbot_meta.db)")

    runpy.run_path("bot.py", run_name="__main__")
