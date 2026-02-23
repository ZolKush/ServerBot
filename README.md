# Server Bot

Telegram bot for server status, maintenance notifications, tickets, user management, Docker checks, and fail2ban log summaries.

## Secrets

Secrets are stored in a separate file and are required at startup:

- Default path: `app/env.secrets`
- Optional override: `SECRETS_ENV_PATH=/path/to/env.secrets`
- Template: `app/env.secrets.example`

Required keys in `env.secrets`:

```env
BOT_TOKEN=...
AUTH_PASSWORD=...
ADMIN_PASSWORD=...
```

The bot will fail to start if the file is missing or any key is empty.

## Config

Non-secret config stays in `app/.env` (see `app/.env.example`).

## Run (Debian, current style)

```bash
sudo -u maintbot bash -lc 'cd /opt/maintbot/app && . /opt/maintbot/.venv/bin/activate && nohup python main.py >> /opt/maintbot/app/bot.log 2>&1 &'
```

## Recommended file permissions

```bash
sudo chown maintbot:maintbot /opt/maintbot/app/env.secrets
sudo chmod 600 /opt/maintbot/app/env.secrets
```

## Install

```bash
pip install -r app/requirements.txt
```

## Logging

Optional runtime env vars:

- `LOG_LEVEL=INFO|DEBUG|...`
- `LOG_JSON=true` (structured JSON logs)

## Quick check

```bash
python -m compileall app
```
