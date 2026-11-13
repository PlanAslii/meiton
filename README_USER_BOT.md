# Phoenix Config Vault v1.1

Professional user-facing bot. Modules:
- `user_bot.py` — Telegram UI + admin panel
- `vault_core.py` — pool, rate-limit, progress bar, Xray batch test
- `xray_tester.py` — shared with main bot

## Railway setup
1. Second service, same repo/volume mount `/data`
2. Env:
```
USER_BOT_TOKEN=<third bot token>
API_ID=...
API_HASH=...
ADMIN_USER_ID=...
DB_FILE=/data/bot_data.db
XRAY_BIN=/data/xray
```
3. Main bot must also use `DB_FILE=/data/bot_data.db`
4. Start: `python user_bot.py`

## Features
- Progress bar during Xray tests
- Country / protocol filters
- Fast export or live Xray test
- Force-join lock (admin)
- Bot on/off (admin)
- Rate limit per user
- TXT delivery with metadata header
- Empty-pool friendly message
