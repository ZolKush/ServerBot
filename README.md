# MaintBot

Telegram-бот для администрирования серверов, выдачи подписок пользователям и обработки обращений в поддержку.

Документ пересобран по текущему состоянию проекта. Последнее изменение прежнего `README.md` в git: коммит `940a2a0d05048c2ce8c450b20e44bb5c9ad13fbf` от `2026-04-24 20:39:32 +0300`, автор `kirill`, сообщение `Add systemd deployment support and local sudo fallbacks`.

## Назначение

Бот работает в личных сообщениях Telegram. Пользователь авторизуется паролем, получает меню, может смотреть статус сервера, получить назначенную подписку и создать тикет. Администратор получает расширенные разделы: пользователи, рассылки, подписки, техработы, Docker, UFW, fail2ban и диагностика серверов.

Проект рассчитан на запуск как long-running service через `python-telegram-bot` и `JobQueue`. Состояние хранится локально в JSON-файлах, а проверки серверов выполняются локально или по SSH.

## Возможности

Для пользователей:

- авторизация через `/auth пароль`;
- главное меню `/start` или `/menu`;
- просмотр краткого статуса сервера;
- получение своей подписки через меню или `/subscription`;
- создание одного открытого тикета с текстом, фото или файлом;
- ответ по тикету после ответа администратора;
- получение уведомлений о техработах.

Для администраторов:

- все пользовательские действия;
- выбор сервера при нескольких настроенных серверах;
- просмотр uptime, RAM, disk, UFW, DNS и Docker;
- просмотр Docker `inspect` и `logs`;
- просмотр fail2ban tail и ручной digest за сутки;
- ежедневная рассылка fail2ban digest администраторам;
- управление пользователями: фильтры, бан/разбан, оплата, nickname;
- массовая рассылка всем авторизованным пользователям;
- назначение подписки без отправки или с немедленной отправкой;
- объявление активных техработ;
- планирование техработ с уведомлением за 30 минут и при старте;
- продление и завершение активных техработ;
- обработка тикетов с назначением исполнителя, ответами и закрытием;
- в `BOT_MODE=mixed`: статус нод из RemnaWave `/metrics`, SSH-диагностика и ручное обновление disk/UFW.

## Структура

```text
.
├── app/
│   ├── main.py                    # точка входа, handlers, jobs
│   ├── __main__.py                # запуск через python -m app
│   ├── config.py                  # публичный слой настроек
│   ├── settings.py                # Pydantic settings, secrets, список серверов
│   ├── storage.py                 # JSON-хранилище, миграции, atomic write
│   ├── models.py                  # TypedDict-модели тикетов, пользователей, техработ
│   ├── constants.py               # подписи пунктов меню
│   ├── logging_setup.py           # обычные или JSON-логи
│   ├── handlers/                  # Telegram-сценарии
│   ├── services/                  # системные, SSH, Docker, DNS, fail2ban, metrics сервисы
│   ├── requirements.txt
│   └── requirements-dev.txt
├── data/
│   ├── user_data.json             # пользователи и подписки
│   └── important_data.json        # тикеты, техработы, DNS/cache
├── deploy/
│   └── maintbot.service           # systemd unit
├── pyproject.toml                 # package/dev metadata
├── .gitignore
├── .gitattributes
└── README.md
```

Локальные `.env`, `env.secrets`, `.venv`, `.idea`, `.claude`, `__pycache__` и runtime-данные не предназначены для публикации. В текущем рабочем дереве шаблоны `app/.env.example` и `app/env.secrets.example` удалены, но в истории git они есть.

## Принцип работы

`app/main.py` строит `Application`, подключает `PicklePersistence` в `data/ptb_persistence`, регистрирует команды, callback handlers и несколько `ConversationHandler`. Затем запускает polling с `drop_pending_updates=True`.

На старте `app/config.py` импортирует `app/settings.py`, настраивает логирование и экспортирует нормализованные значения. `settings.py` читает:

- `app/.env` или путь из `ENV_PATH`;
- `app/env.secrets` или путь из `SECRETS_ENV_PATH`;
- переменные окружения процесса.

Секреты `BOT_TOKEN`, `AUTH_PASSWORD`, `ADMIN_PASSWORD` валидируются отдельно. `BOT_TOKEN` обязателен, а из пользовательского и админского паролей должен быть задан хотя бы один.

Серверы собираются в словарь `SERVERS`. Всегда создаётся локальный сервер. Удалённые серверы добавляются из plural-настроек `REMOTE_SERVER_SSH_TARGETS`, `REMOTE_SERVER_CODES`, `REMOTE_SERVER_LABELS`, `REMOTE_SERVER_FLAGS`, `REMOTE_SERVER_EXPECTED_A_IPS`, `REMOTE_SERVER_DOMAINS` и `REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER`. Legacy-настройки одного удалённого сервера тоже поддерживаются.

Состояние лежит в двух JSON-файлах. `storage.py` мигрирует старые схемы, нормализует пользователей, пишет через временный файл и на Linux пытается выставлять права `0600`.

## Основные модули

`app/handlers/auth.py`: `/start`, `/help`, `/auth`, `/logout`, rate-limit неудачных попыток авторизации, удаление сообщения с паролем.

`app/handlers/common.py`: проверки доступа, роли, меню, HTML escaping, форматирование дат, массовая отправка с retry и ограничением concurrency.

`app/handlers/status.py`: статус локального или SSH-сервера, DNS cache, ручной DNS refresh, UFW, mixed-mode через RemnaWave metrics, ручная SSH-диагностика.

`app/handlers/status_format.py` и `status_models.py`: структура и форматирование сообщений статуса.

`app/handlers/docker.py`: админское меню контейнеров, `inspect`, `logs`, проверка допустимых имён контейнеров из настроек сервера.

`app/handlers/fail2ban.py`: tail логов, digest за период, ежедневный digest по каждому серверу, отдельный state-файл digest на сервер.

`app/handlers/maint.py` и `maint_helpers.py`: активные и запланированные техработы, scope по серверу или всем серверам, уведомления пользователям и админам, продление и завершение.

`app/handlers/tickets.py`: создание тикетов, история сообщений, вложения Telegram, назначение исполнителя, ответы пользователя и администратора, закрытие.

`app/handlers/users.py`, `users_ui.py`, `users_constants.py`: список пользователей, фильтры, карточка пользователя, рассылки, личные сообщения, nickname, оплата, подписки.

`app/handlers/subscription.py`: выдача подписки пользователю. Короткий текст отправляется inline, длинный как `.txt` файл.

`app/services/system_process.py`: безопасный async-запуск subprocess с timeout.

`app/services/system_metrics.py`: uptime, memory и disk локального сервера через `/proc`, `free`, `df`.

`app/services/system_dns.py`: A-записи через `aiodns` с custom resolvers и fallback на `socket.getaddrinfo`.

`app/services/system_ufw.py`: UFW status и правила, с fallback на `sudo -n`.

`app/services/docker_service.py`: локальный Docker `ps`, `inspect`, `logs`, с fallback на `sudo -n`.

`app/services/remote_service.py`: SSH-команды для удалённых серверов, сбор status bundle, Docker, fail2ban, UFW и disk.

`app/services/system_fail2ban.py`: tail логов, sudo fallback, parser fail2ban events, JSON state.

`app/services/remnawave_metrics.py`: получение Prometheus-метрик RemnaWave, cache, parsing node metrics, online/offline, RAM, uptime, online users.

## Конфигурация

Минимальный `app/env.secrets`:

```env
BOT_TOKEN=123456:telegram-token
AUTH_PASSWORD=user-password
ADMIN_PASSWORD=admin-password
```

Минимальный `app/.env` для локального запуска из корня проекта:

```env
TZ=Europe/Moscow
LOG_LEVEL=INFO
LOG_JSON=false

USER_DATA_PATH=data/user_data.json
IMPORTANT_DATA_PATH=data/important_data.json

LOCAL_SERVER_CODE=local
LOCAL_SERVER_LABEL=Local server
LOCAL_SERVER_FLAG=
MONITOR_CONTAINERS=
EXPECTED_A_IP=
CHECK_A_DOMAINS=

REMOTE_SERVER_ENABLED=false
```

Пример для локального сервера и нескольких SSH-серверов:

```env
TZ=Europe/Moscow
LOG_LEVEL=INFO
LOG_JSON=false

USER_DATA_PATH=/opt/maintbot/data/user_data.json
IMPORTANT_DATA_PATH=/opt/maintbot/data/important_data.json

LOCAL_SERVER_CODE=nl
LOCAL_SERVER_LABEL=Netherlands(Bot)
LOCAL_SERVER_FLAG=NL
MONITOR_CONTAINERS=remnanode,remnawave-nginx
EXPECTED_A_IP=203.0.113.20
CHECK_A_DOMAINS=nl.example.com

DNS_RESOLVERS=1.1.1.1,8.8.8.8
FAIL2BAN_LOG_PATH=/var/log/fail2ban.log
FAIL2BAN_DAILY_AT=12:00
DNS_DAILY_REFRESH_AT=03:05
DNS_STARTUP_REFRESH_DELAY_SEC=5

REMOTE_SERVER_ENABLED=true
REMOTE_SERVER_CODES=main,ru1,ru2
REMOTE_SERVER_LABELS=Russia(Main),Russia(S1),Russia(S2)
REMOTE_SERVER_FLAGS=RU,RU,RU
REMOTE_SERVER_SSH_TARGETS=maintbot@203.0.113.10:1606,maintbot@203.0.113.11:1606,maintbot@203.0.113.12:1606
REMOTE_SERVER_EXPECTED_A_IPS=203.0.113.10,203.0.113.11,203.0.113.12
REMOTE_SERVER_DOMAINS=main.example.com;ru1.example.com;ru2.example.com
REMOTE_SERVER_FAIL2BAN_LOG_PATH=/var/log/fail2ban.log
REMOTE_SERVER_MONITOR_CONTAINERS=remnanode,remnawave-nginx
REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER=remnawave,remnawave-db,remnawave-redis;remnanode,remnawave-nginx;remnanode,remnawave-nginx
```

Для RemnaWave mixed-mode:

```env
BOT_MODE=mixed
REMNAWAVE_METRICS_URL=https://panel.example.com/metrics
REMNAWAVE_METRICS_USER=
REMNAWAVE_METRICS_PASS=
REMNAWAVE_METRICS_TIMEOUT_SEC=3
REMNAWAVE_METRICS_CACHE_TTL_SEC=8
REMNAWAVE_HIDDEN_UUIDS=

LOCAL_SERVER_REMNAWAVE_UUID=
REMOTE_SERVER_REMNAWAVE_UUIDS=uuid-for-main,uuid-for-ru1,uuid-for-ru2
DAILY_NODE_STATUS_REFRESH_AT=12:00
```

В `BOT_MODE=ssh` статус строится через локальные команды и SSH. В `BOT_MODE=mixed` для серверов с RemnaWave UUID uptime/RAM/online берутся из `/metrics`, а disk/UFW берутся из кэша, который обновляется ежедневным job или вручную администратором через SSH.

## Переменные окружения

Базовые:

- `ENV_PATH`, `SECRETS_ENV_PATH`;
- `TZ`;
- `LOG_LEVEL`, `LOG_JSON`;
- `USER_DATA_PATH`, `IMPORTANT_DATA_PATH`, `CONFIG_PATH`.

Авторизация и защита:

- `BOT_TOKEN`;
- `AUTH_PASSWORD`;
- `ADMIN_PASSWORD`;
- `AUTH_FAIL_WINDOW_SEC`;
- `AUTH_MAX_FAILS_IN_WINDOW`;
- `AUTH_LOCKOUT_SEC`;
- `AUTH_PRUNE_INTERVAL_SEC`;
- `ERROR_NOTIFY_INTERVAL_SEC`.

Локальный сервер:

- `LOCAL_SERVER_CODE`;
- `LOCAL_SERVER_LABEL`;
- `LOCAL_SERVER_FLAG`;
- `EXPECTED_A_IP`;
- `CHECK_A_DOMAINS`;
- `MONITOR_CONTAINERS`;
- `FAIL2BAN_LOG_PATH`;
- `FAIL2BAN_STATE_PATH`.

Удалённые серверы:

- `REMOTE_SERVER_ENABLED`;
- `REMOTE_SERVER_CODES`;
- `REMOTE_SERVER_LABELS`;
- `REMOTE_SERVER_FLAGS`;
- `REMOTE_SERVER_SSH_TARGETS`;
- `REMOTE_SERVER_EXPECTED_A_IPS`;
- `REMOTE_SERVER_DOMAINS`;
- `REMOTE_SERVER_FAIL2BAN_LOG_PATH`;
- `REMOTE_SERVER_MONITOR_CONTAINERS`;
- `REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER`.

Legacy-настройки одного удалённого сервера:

- `REMOTE_SERVER_CODE`;
- `REMOTE_SERVER_LABEL`;
- `REMOTE_SERVER_FLAG`;
- `REMOTE_SERVER_SSH_TARGET`;
- `REMOTE_SERVER_EXPECTED_A_IP`;
- `REMOTE_SERVER_CHECK_A_DOMAINS`.

DNS, jobs и subprocess:

- `DNS_RESOLVERS`;
- `FAIL2BAN_DAILY_AT`;
- `FAIL2BAN_DIGEST_TAIL_LINES`;
- `FAIL2BAN_DIGEST_MAX_BYTES`;
- `DNS_DAILY_REFRESH_AT`;
- `DNS_STARTUP_REFRESH_DELAY_SEC`;
- `MAINT_RESTART_NOTIFY_DELAY_SEC`;
- `MAINT_RESTART_REMINDER_INTERVAL_SEC`;
- `SUBPROC_SHORT_TIMEOUT`;
- `SUBPROC_MEDIUM_TIMEOUT`.

SSH:

- `SSH_STRICT_HOST_KEY_CHECKING`;
- `SSH_KNOWN_HOSTS_FILE`.

Рассылки:

- `BROADCAST_MAX_CONCURRENCY`;
- `BROADCAST_MAX_ATTEMPTS`.

RemnaWave:

- `BOT_MODE`;
- `REMNAWAVE_METRICS_URL`;
- `REMNAWAVE_METRICS_USER`;
- `REMNAWAVE_METRICS_PASS`;
- `REMNAWAVE_METRICS_TIMEOUT_SEC`;
- `REMNAWAVE_METRICS_CACHE_TTL_SEC`;
- `REMNAWAVE_HIDDEN_UUIDS`;
- `LOCAL_SERVER_REMNAWAVE_UUID`;
- `REMOTE_SERVER_REMNAWAVE_UUIDS`;
- `DAILY_NODE_STATUS_REFRESH_AT`.

## Данные

`data/user_data.json`:

```json
{
  "schema_version": 1,
  "authorized_users": {
    "1111111": {
      "user_id": 1111111,
      "role": "admin",
      "nickname": "Admin",
      "username": "admin_username",
      "first_name": "Admin",
      "last_name": null,
      "auth_at": "2026-01-01T12:00:00+03:00",
      "enabled": true,
      "is_paid": true,
      "subscription_text": "vless://...",
      "subscription_updated_at": "2026-01-01T12:10:00+03:00",
      "subscription_updated_by_id": 1111111,
      "subscription_updated_by_name": "Admin"
    }
  }
}
```

`data/important_data.json`:

```json
{
  "schema_version": 1,
  "tickets_seq": 1,
  "tickets": {},
  "maintenance": {},
  "scheduled_maintenance": {},
  "dns_status": {},
  "daily_node_status": {}
}
```

Код умеет загрузить неполный старый JSON и дописать недостающие ключи при следующем сохранении.

## Установка

Требуется Python 3.10+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r app\requirements.txt
```

Создайте `app/.env` и `app/env.secrets`, затем проверьте синтаксис:

```bash
python -m compileall app
```

Локальный запуск из корня проекта:

```bash
python -m app.main
```

Альтернативно, если проект установлен как package:

```bash
maintbot
```

## Развёртывание через systemd

Рекомендуемый каталог:

```text
/opt/maintbot/
  app/
  data/
  deploy/
  .venv/
```

Unit-файл лежит в `deploy/maintbot.service`:

```ini
[Unit]
Description=MaintBot Telegram Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=maintbot
WorkingDirectory=/opt/maintbot
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/maintbot/.venv/bin/python -m app.main
Restart=always
RestartSec=5
TimeoutStopSec=30
LogsDirectory=maintbot
StandardOutput=append:/var/log/maintbot/bot.log
StandardError=append:/var/log/maintbot/bot.log

[Install]
WantedBy=multi-user.target
```

Пример установки:

```bash
sudo useradd -r -m -d /opt/maintbot -s /bin/bash maintbot
sudo chown -R maintbot:maintbot /opt/maintbot
sudo chmod 600 /opt/maintbot/app/env.secrets
sudo cp /opt/maintbot/deploy/maintbot.service /etc/systemd/system/maintbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now maintbot.service
```

Для Docker, UFW и fail2ban при запуске не от root нужен доступ к командам. Код пробует обычный вызов и fallback через `sudo -n`, поэтому для пользователя сервиса должен быть настроен `NOPASSWD`, если прямых прав недостаточно:

```bash
sudo usermod -aG sudo maintbot
echo 'maintbot ALL=(ALL) NOPASSWD: ALL' | sudo tee /etc/sudoers.d/maintbot
sudo chmod 440 /etc/sudoers.d/maintbot
```

Более строгий вариант лучше ограничить конкретными командами `docker`, `ufw`, `tail`, `stat`.

## SSH-серверы

`REMOTE_SERVER_SSH_TARGETS` использует формат `user@host` или `user@host:port`. SSH запускается с:

- `BatchMode=yes`;
- `ConnectTimeout`;
- `LogLevel=ERROR`;
- `StrictHostKeyChecking` из `SSH_STRICT_HOST_KEY_CHECKING`, по умолчанию `accept-new`;
- опциональным `UserKnownHostsFile` из `SSH_KNOWN_HOSTS_FILE`.

На удалённой стороне бот выполняет `sh -c`, читает `/proc/uptime`, `/proc/meminfo`, `df -B1 /`, `ufw status`, `docker ps`, `docker inspect`, `docker logs`, `tail` и `stat` для fail2ban. Если нужны повышенные права, на удалённом сервере тоже должен работать `sudo -n`.

## Фоновые задачи

При доступном `JobQueue` регистрируются:

- `fail2ban_digest`: ежедневная выжимка fail2ban администраторам;
- `dns_daily_refresh`: ежедневное обновление DNS cache;
- `dns_refresh_startup`: одноразовый DNS refresh после старта;
- `daily_node_status_refresh`: daily disk/UFW refresh в `BOT_MODE=mixed`;
- `daily_node_status_startup`: startup refresh для mixed-mode;
- `maint_active_reminder`: периодическое напоминание админам об активных техработах;
- `maint_schedule_tick`: проверка запланированных техработ каждую минуту;
- `auth_prune`: очистка памяти rate-limit авторизации.

## Команды

- `/start`;
- `/menu`;
- `/help`;
- `/auth пароль`;
- `/login пароль`;
- `/logout`;
- `/health`;
- `/subscription`;
- `/ticket`;
- `/users`;
- `/maint`;
- `/fail2ban`;
- `/cancel`.

## Логирование

Логирование настраивается через `LOG_LEVEL` и `LOG_JSON`.

При `LOG_JSON=false` используется текстовый формат. При `LOG_JSON=true` каждая запись выводится JSON-объектом с `ts`, `level`, `logger`, `msg` и полями вроде `user_id`, `chat_id`, `server_key`, `action`, если они переданы.

`httpx` и `httpcore` понижены до `WARNING`.

Просмотр логов systemd-варианта:

```bash
sudo tail -n 200 /var/log/maintbot/bot.log
sudo tail -f /var/log/maintbot/bot.log
sudo systemctl status maintbot.service --no-pager -l
```

## Проверка

```bash
python -m compileall app
```

Для разработки доступны зависимости:

```bash
pip install -r app/requirements-dev.txt
```

`pyproject.toml` задаёт package `maintbot`, script entry point `maintbot = app.main:main`, dev-зависимости `pytest`, `pytest-asyncio`, `ruff`, `mypy` и `ruff` line length `120`.

## Типичные проблемы

`BOT_TOKEN` или пароли не заданы:

Проверьте `app/env.secrets`, `app/.env`, `ENV_PATH`, `SECRETS_ENV_PATH` и переменные окружения процесса.

`Conflict: terminated by other getUpdates request`:

Один Telegram token уже используется другим процессом. Оставьте только один экземпляр бота.

`sudo: a password is required`:

Код вызвал `sudo -n`, но пользователь сервиса не имеет `NOPASSWD`. Настройте sudoers или дайте прямые права.

Docker/UFW/fail2ban недоступны:

Проверьте наличие команд на локальном или удалённом сервере, права пользователя, SSH-доступ и `sudo -n`.

DNS показывает "нет свежих данных":

DNS-статус хранится в cache. Нажмите "Обновить DNS статус" или дождитесь `dns_daily_refresh`.

Mixed-mode показывает ноду offline или ошибку metrics:

Проверьте `REMNAWAVE_METRICS_URL`, basic auth, доступ к `/metrics`, UUID ноды и `REMNAWAVE_HIDDEN_UUIDS`.

Тикет не создаётся:

У пользователя может уже быть открытый тикет, или нет авторизованных администраторов.

## Безопасность

- Не храните реальные `BOT_TOKEN`, пароли, SSH targets с приватными деталями и подписки в публичном репозитории.
- `data/user_data.json` содержит Telegram ID, имена пользователей и подписки. Относитесь к нему как к чувствительным данным.
- `data/important_data.json` может содержать тексты тикетов и вложения через Telegram file id.
- Не запускайте локально production token, если systemd-сервис уже работает.
- Не редактируйте JSON во время активной работы бота без понимания схемы.
- Для production ограничьте sudoers конкретными командами вместо полного `NOPASSWD: ALL`.
