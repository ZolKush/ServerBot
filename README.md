# MaintBot

Telegram-бот для администрирования серверов и поддержки пользователей.

Бот работает в личных сообщениях Telegram, хранит состояние в JSON-файлах и использует `python-telegram-bot` с `JobQueue` для фоновых задач. Основной сценарий: пользователь авторизуется, получает меню, может открыть тикет и получить подписку, а администратор управляет пользователями, техработами, статусами серверов, Docker и fail2ban.

## Что умеет бот

### Для пользователей

- авторизация по паролю `/auth`
- просмотр главного меню
- просмотр своей подписки через `📦 Моя подписка`
- создание тикета в поддержку
- ответ администратору внутри тикета, если администратор уже ответил
- получение уведомлений о техработах
- просмотр краткого статуса сервера

### Для администраторов

- всё, что доступно пользователю
- просмотр статуса локального и удалённого сервера
- просмотр UFW
- просмотр Docker `inspect` и `logs`
- просмотр fail2ban tail и суточной выжимки
- ежедневная автоматическая выжимка fail2ban
- управление пользователями
- массовая рассылка всем авторизованным пользователям
- назначение и отправка подписок
- скрытое обновление меню пользователя
- объявление активных техработ
- планирование техработ с уведомлением за 30 минут и в момент старта
- обработка тикетов с исполнителем, ответами и закрытием

## Архитектура

### Точка входа

- [app/main.py](/C:/Users/kiril/Documents/server_bot/app/main.py)

Там:

- создаётся `Application`
- регистрируются команды, callback и conversation flow
- подключаются фоновые задачи `JobQueue`
- включается polling c `drop_pending_updates=True`

### Конфигурация

- [app/settings.py](/C:/Users/kiril/Documents/server_bot/app/settings.py)
- [app/config.py](/C:/Users/kiril/Documents/server_bot/app/config.py)
- [app/logging_setup.py](/C:/Users/kiril/Documents/server_bot/app/logging_setup.py)

`settings.py` читает:

- `app/.env`
- `app/env.secrets`
- переменные окружения процесса

Секреты:

- `BOT_TOKEN`
- `AUTH_PASSWORD`
- `ADMIN_PASSWORD`

Поддерживается и `app/.env`, и `app/env.secrets`, но для продакшена секреты лучше держать в `app/env.secrets`.

### Хранилище данных

- [app/storage.py](/C:/Users/kiril/Documents/server_bot/app/storage.py)
- [data/user_data.json](/C:/Users/kiril/Documents/server_bot/data/user_data.json)
- [data/important_data.json](/C:/Users/kiril/Documents/server_bot/data/important_data.json)

`user_data.json` хранит:

- авторизованных пользователей
- роли `user/admin`
- флаг `enabled`
- флаг оплаты `is_paid`
- nickname
- подписку пользователя и метаданные её обновления

`important_data.json` хранит:

- счётчик тикетов `tickets_seq`
- сами тикеты `tickets`
- активные техработы `maintenance`
- запланированные техработы `scheduled_maintenance`
- кэш DNS-статуса `dns_status`

Запись идёт атомарно через временный файл.

### Хендлеры

- [app/handlers/auth.py](/C:/Users/kiril/Documents/server_bot/app/handlers/auth.py)  
  Авторизация, старт, помощь, logout.

- [app/handlers/common.py](/C:/Users/kiril/Documents/server_bot/app/handlers/common.py)  
  Общие helper’ы, доступ, меню, массовая отправка.

- [app/handlers/status.py](/C:/Users/kiril/Documents/server_bot/app/handlers/status.py)  
  Статус серверов, DNS refresh, UFW.

- [app/handlers/docker.py](/C:/Users/kiril/Documents/server_bot/app/handlers/docker.py)  
  Docker-меню, inspect и logs.

- [app/handlers/fail2ban.py](/C:/Users/kiril/Documents/server_bot/app/handlers/fail2ban.py)  
  Tail логов, digest за сутки, ежедневная рассылка админам.

- [app/handlers/maint.py](/C:/Users/kiril/Documents/server_bot/app/handlers/maint.py)  
  Активные и запланированные техработы, продление, завершение, автоматические уведомления.

- [app/handlers/tickets.py](/C:/Users/kiril/Documents/server_bot/app/handlers/tickets.py)  
  Создание тикетов, взятие в работу, ответы админа и пользователя, закрытие, логирование тикетного потока.

- [app/handlers/users.py](/C:/Users/kiril/Documents/server_bot/app/handlers/users.py)  
  Админ-панель пользователей, рассылки, никнеймы, оплата, подписки, обновление меню.

- [app/handlers/subscription.py](/C:/Users/kiril/Documents/server_bot/app/handlers/subscription.py)  
  Выдача пользователю его подписки.

### Сервисы

- [app/services/system_process.py](/C:/Users/kiril/Documents/server_bot/app/services/system_process.py)  
  Базовый async запуск процессов.

- [app/services/system_metrics.py](/C:/Users/kiril/Documents/server_bot/app/services/system_metrics.py)  
  Uptime, RAM, диск.

- [app/services/system_dns.py](/C:/Users/kiril/Documents/server_bot/app/services/system_dns.py)  
  DNS A-записи, с `aiodns` и fallback на системный resolver.

- [app/services/system_ufw.py](/C:/Users/kiril/Documents/server_bot/app/services/system_ufw.py)  
  Чтение статуса и правил UFW.

- [app/services/system_fail2ban.py](/C:/Users/kiril/Documents/server_bot/app/services/system_fail2ban.py)  
  Парсинг fail2ban логов и работа с JSON state-файлами.

- [app/services/docker_service.py](/C:/Users/kiril/Documents/server_bot/app/services/docker_service.py)  
  Локальная работа с Docker.

- [app/services/remote_service.py](/C:/Users/kiril/Documents/server_bot/app/services/remote_service.py)  
  SSH-вызовы для удалённого сервера: статус, Docker, fail2ban.

- [app/services/system_service.py](/C:/Users/kiril/Documents/server_bot/app/services/system_service.py)  
  Переэкспорт системных helper’ов.

## Меню и логика

### Главное меню пользователя

- `📊 Статус сервера`
- `📦 Моя подписка`
- `🎫 Создать тикет`
- `ℹ️ Помощь`

### Главное меню администратора

- `📊 Статус сервера`
- `📦 Моя подписка`
- `🎫 Создать тикет`
- `👥 Пользователи`
- `🛠 Техработы`
- `ℹ️ Помощь`

### Тикеты

Поток тикета такой:

1. Пользователь создаёт тикет: тема -> срочность -> описание -> подтверждение.
2. Тикет сохраняется в `important_data.json`.
3. Все админы получают карточку тикета.
4. Любой админ может взять тикет в работу.
5. После взятия у тикета появляется исполнитель.
6. Ответить пользователю и закрыть тикет может только исполнитель.
7. После ответа администратора пользователю доступна кнопка ответа.
8. После ответа пользователя право хода возвращается администратору.
9. После закрытия тикета пользователь больше не может писать по нему.

### Подписки

Подписка хранится прямо в карточке пользователя в `user_data.json`.

Админ может:

- `💾 Назначить подписку`  
  Только сохранить в базе.

- `📤 Отправить подписку`  
  Сохранить в базе и сразу отправить пользователю.

Пользователь в любой момент может открыть `📦 Моя подписка` и получить актуальную версию.

### Техработы

Админский раздел техработ поддерживает два режима:

- `🚨 Объявить техработы`  
  Немедленно создаёт активные техработы и рассылает уведомления.

- `🗓 Запланировать техработы`  
  Создаёт запись в `scheduled_maintenance`, после чего бот сам:
  - отправляет уведомление за 30 минут до старта
  - отправляет уведомление в момент старта
  - переводит план в активные техработы

### Пользователи

Админская карточка пользователя поддерживает:

- личное сообщение
- смену nickname
- переключение оплаты
- назначение/отправку подписки
- обновление меню пользователя
- бан/разбан обычных пользователей

## Конфигурация

### Обязательные файлы

- `app/.env`
- `app/env.secrets`

Шаблоны:

- [app/.env.example](/C:/Users/kiril/Documents/server_bot/app/.env.example)
- [app/env.secrets.example](/C:/Users/kiril/Documents/server_bot/app/env.secrets.example)

### Минимальный `app/env.secrets`

```env
BOT_TOKEN=123456:telegram-token
AUTH_PASSWORD=user-password
ADMIN_PASSWORD=admin-password
```

### Рекомендуемый `app/.env` для локального запуска из репозитория

```env
TZ=Europe/Moscow
LOG_LEVEL=INFO
LOG_JSON=false

USER_DATA_PATH=data/user_data.json
IMPORTANT_DATA_PATH=data/important_data.json

LOCAL_SERVER_CODE=nl
LOCAL_SERVER_LABEL=Netherlands
EXPECTED_A_IP=127.0.0.1
CHECK_A_DOMAINS=example.com
MONITOR_CONTAINERS=remnawave,remnawave-db,remnawave-redis,remnanode,remnawave-nginx
FAIL2BAN_LOG_PATH=/var/log/fail2ban.log

REMOTE_SERVER_ENABLED=false
```

### Основные параметры `app/.env`

#### Базовые

- `TZ`
- `LOG_LEVEL`
- `LOG_JSON`
- `USER_DATA_PATH`
- `IMPORTANT_DATA_PATH`

#### Локальный сервер

- `LOCAL_SERVER_CODE`
- `LOCAL_SERVER_LABEL`
- `EXPECTED_A_IP`
- `CHECK_A_DOMAINS`
- `MONITOR_CONTAINERS`
- `FAIL2BAN_LOG_PATH`

#### Удалённый сервер

- `REMOTE_SERVER_ENABLED`
- `REMOTE_SERVER_CODE`
- `REMOTE_SERVER_LABEL`
- `REMOTE_SERVER_SSH_TARGET`
- `REMOTE_SERVER_EXPECTED_A_IP`
- `REMOTE_SERVER_CHECK_A_DOMAINS`
- `REMOTE_SERVER_FAIL2BAN_LOG_PATH`
- `REMOTE_SERVER_MONITOR_CONTAINERS`

#### DNS и фоновые задачи

- `DNS_RESOLVERS`
- `FAIL2BAN_DAILY_AT`
- `DNS_DAILY_REFRESH_AT`
- `DNS_STARTUP_REFRESH_DELAY_SEC`
- `MAINT_RESTART_NOTIFY_DELAY_SEC`

#### Таймауты

- `SUBPROC_SHORT_TIMEOUT`
- `SUBPROC_MEDIUM_TIMEOUT`

### Что реально читает код из `.env`

Да, бот реально использует значения из `.env`:

- пути к JSON-хранилищам
- логирование
- настройки локального и удалённого серверов
- DNS resolver’ы и домены
- пути fail2ban
- расписание фоновых задач
- SSH target удалённого сервера

Секреты тоже могут читаться из `.env`, но для продакшена лучше держать их в `env.secrets`.

## Установка

### 1. Подготовить Python

Рекомендуется Python 3.11+.

### 2. Создать venv

```bash
python -m venv .venv
```

Linux:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Установить зависимости

```bash
pip install -r app/requirements.txt
```

### 4. Создать конфиги

Скопируйте шаблоны:

```bash
cp app/.env.example app/.env
cp app/env.secrets.example app/env.secrets
```

Потом заполните реальные значения.

### 5. Проверить синтаксис

```bash
python -m compileall app
```

### 6. Локовый тест запуска

Из корня проекта:

```bash
python -m app.main
```

## Развёртывание на Linux сервере

Ниже пример для каталога `/opt/maintbot`.

### Рекомендуемая структура

```text
/opt/maintbot/
  app/
  data/
  .venv/
```

### Рекомендуемые права на секреты

```bash
chown maintbot:maintbot /opt/maintbot/app/env.secrets
chmod 600 /opt/maintbot/app/env.secrets
```

### Рекомендуемые значения путей на сервере

```env
USER_DATA_PATH=/opt/maintbot/data/user_data.json
IMPORTANT_DATA_PATH=/opt/maintbot/data/important_data.json
```

## Запуск через systemd

Рекомендуемый способ запуска: только через `systemd`.

Пример unit-файла:

```ini
[Unit]
Description=MaintBot Telegram Bot
After=network.target

[Service]
Type=simple
User=maintbot
WorkingDirectory=/opt/maintbot
ExecStart=/opt/maintbot/.venv/bin/python -m app.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

После создания:

```bash
sudo systemctl daemon-reload
sudo systemctl enable maintbot.service
sudo systemctl start maintbot.service
```

## Проверка работы

### Проверка статуса сервиса

```bash
sudo systemctl status maintbot.service --no-pager -l
```

### Просмотр логов

```bash
sudo journalctl -u maintbot.service -n 200 --no-pager
sudo journalctl -u maintbot.service -f
```

### Признаки успешного запуска

- `Bot started`
- `Application started`
- `Scheduler started`
- добавлены jobs `fail2ban_digest`, `dns_daily_refresh`, `dns_refresh_startup`, `maint_restart_notify`, `maint_schedule_tick`

## Логирование

Логирование настраивается через:

- `LOG_LEVEL`
- `LOG_JSON`

Если `LOG_JSON=false`, логи обычные текстовые.  
Если `LOG_JSON=true`, каждая запись идёт JSON-объектом.

Отдельно логируются:

- авторизация и ошибки авторизации
- DNS refresh
- запуск, продление и завершение техработ
- планирование техработ
- действия по пользователям
- выдача подписки
- весь жизненный цикл тикетов

`httpx` и `httpcore` опущены до `WARNING`, чтобы не светить лишние детали Telegram API.

## Типичные проблемы

### `Conflict: terminated by other getUpdates request`

Причина: бот запущен более чем в одном процессе с одним и тем же `BOT_TOKEN`.

Что делать:

- оставить только один экземпляр
- не запускать вручную `python main.py` или `python -m app.main`, если сервис уже запущен
- для локальной разработки использовать отдельный токен

### Бот не видит локальные `data/*.json`

Проверьте `USER_DATA_PATH` и `IMPORTANT_DATA_PATH`.

Если хотите использовать файлы из репозитория, задайте:

```env
USER_DATA_PATH=data/user_data.json
IMPORTANT_DATA_PATH=data/important_data.json
```

### Нет данных по удалённому серверу

Проверьте:

- `REMOTE_SERVER_ENABLED=true`
- `REMOTE_SERVER_SSH_TARGET`
- доступ по SSH без интерактивного пароля
- наличие Docker/UFW/fail2ban на удалённой машине

### Не отправляются уведомления пользователям

Возможные причины:

- пользователь не запускал бота
- пользователь заблокировал бота
- Telegram вернул ошибку доставки

Эти случаи попадают в лог.

## Формат данных

### `data/user_data.json`

Пример:

```json
{
  "schema_version": 1,
  "authorized_users": {
    "1111111": {
      "user_id": 1111111,
      "role": "admin",
      "nickname": "Кирилл Французов",
      "username": "ZoL_Kush",
      "first_name": "ZoL",
      "last_name": "Kush",
      "auth_at": "2025",
      "enabled": true,
      "is_paid": true
    }
  }
}
```

Дополнительно у пользователя могут храниться:

- `subscription_text`
- `subscription_updated_at`
- `subscription_updated_by_id`
- `subscription_updated_by_name`

### `data/important_data.json`

Содержит:

- `tickets_seq`
- `tickets`
- `maintenance`
- `scheduled_maintenance`
- `dns_status`

Даже если старый JSON ещё не содержит часть ключей, код их домигрирует при загрузке.

## Команды

- `/start`
- `/menu`
- `/help`
- `/auth пароль`
- `/logout`
- `/health`
- `/subscription`
- `/ticket`
- `/users`
- `/maint`
- `/fail2ban`

## Что не стоит делать

- не хранить секреты в публичном репозитории
- не запускать два экземпляра бота с одним токеном
- не редактировать JSON-файлы вручную во время активной работы бота без понимания схемы
- не запускать прод-бота локально тем же токеном, что и на сервере

## Быстрая памятка

Локальный запуск:

```bash
python -m compileall app
python -m app.main
```

Продакшен:

```bash
sudo systemctl restart maintbot.service
sudo journalctl -u maintbot.service -f
```
