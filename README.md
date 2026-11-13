# Phoenix Complete Package

## Files
- bot.py : Main Phoenix collector bot
- user_bot.py : Config Vault user bot
- vault_core.py : Vault core (pool, progress bar, rate limit)
- npvt_decrypt.py : NPVT decrypt module
- xray_tester.py : Xray/TCP tester
- download_xray.py / download_xray.sh : download xray binary
- start.py : run main bot + user bot together
- requirements.txt

## Railway (one service for both bots)
Start command:
```
python start.py
```

Required env vars:
```
API_ID
API_HASH
ADMIN_USER_ID
BOT_TOKEN
BOT_TOKEN_2
SELF_SESSION_STRING
USER_BOT_TOKEN
DB_FILE=/data/bot_data.db
XRAY_BIN=/data/xray
WEB_SECRET_PATH=phoenix
```

Install xray once in console:
```
python3 download_xray.py
cp xray /data/xray
chmod +x /data/xray
```

## Notes
- User bot and main bot share bot_data.db
- Telegram proxy test is separate from Xray test
- Xray test shows disclaimer that server is outside Iran
