# Server Bot

Telegram bot for server administration and user support:

- server status (uptime / RAM / disk / Docker / DNS)
- Docker inspect / logs
- fail2ban (tail / digest)
- maintenance notifications
- user tickets
- user admin panel

The bot is set up for manual deployment and manual startup on a Linux server (no GitHub Actions / auto-deploy).

## Configuration Layout

### `app/.env`
Non-secret settings only:

- server codes / labels
- SSH target
- DNS domains
- log paths
- timeouts, etc.

Template: `app/.env.example`

### `app/env.secrets`
Secrets (required file):

- `BOT_TOKEN`
- `AUTH_PASSWORD`
- `ADMIN_PASSWORD`

Template: `app/env.secrets.example`

Important:

- the bot does not read these keys from `app/.env`
- if `env.secrets` is missing or incomplete, the bot will fail at startup

## Prepare Config Files

Create real config files from the templates:

- `app/.env`
- `app/env.secrets`

Example `app/env.secrets`:

```env
BOT_TOKEN=...
AUTH_PASSWORD=...
ADMIN_PASSWORD=...
```

## Install Dependencies

```bash
pip install -r app/requirements.txt
```

## Run (Debian, manual)

Current working startup command:

```bash
sudo -u maintbot bash -lc 'cd /opt/maintbot/app && . /opt/maintbot/.venv/bin/activate && nohup python main.py >> /opt/maintbot/app/bot.log 2>&1 &'
```

## Recommended Permissions for Secrets File

```bash
sudo chown maintbot:maintbot /opt/maintbot/app/env.secrets
sudo chmod 600 /opt/maintbot/app/env.secrets
```

## What to Upload to the Server

Required:

1. `app/`
2. `app/.env` (without secrets)
3. `app/env.secrets` (with secrets)
4. `app/requirements.txt`

Optional:

1. `README.md`
2. `app/.env.example`
3. `app/env.secrets.example`

Do not upload:

1. `.github/` (if present)
2. `tests/` (if present)
3. `.idea/`
4. local `__pycache__/`

## UI Notes (Current Behavior)

- Main navigation uses inline buttons inside the message
- The bot mostly works in a “single message / single card” flow
- Text input steps (tickets, nicknames, admin messages, config, maintenance duration) still use normal Telegram text messages

## Logging

Optional runtime environment variables:

- `LOG_LEVEL=INFO|DEBUG|WARNING|...`
- `LOG_JSON=true` (structured JSON logs)

Note:

- `httpx/httpcore` logging is reduced to `WARNING`, so Telegram API URLs with the bot token do not end up in `bot.log`

## Quick Check After Update

### Syntax check

```bash
python -m compileall app
```

### Startup check

View log:

```bash
cat /opt/maintbot/app/bot.log
```

Expected signs of a successful startup:

- `Bot started`
- `Application started`
- `Scheduler started` (if `job_queue` is available)

## Security Notes

1. Do not store token/passwords in `app/.env`
2. Do not publish `app/env.secrets`
3. If the token ever appears in logs/screenshots/chat, rotate it immediately

