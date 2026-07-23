# MaintBot

MaintBot — Telegram-бот для наблюдения за локальными и удалёнными серверами, управления доступом и подписками, проведения техработ и общения с поддержкой. Бот работает через long polling, запускается отдельным непривилегированным пользователем и хранит состояние в разделённых локальных JSON-хранилищах.

## Возможности

Пользователь может:

- запросить доступ без общего пользовательского пароля;
- создать тикет с текстом, фото или документом и продолжить переписку;
- запросить одноразовый тест или купить подписку;
- получить персональную ссылку подключения;
- сохранить резервную электронную почту;
- получать объявления, напоминания и уведомления о техработах.

Подписчики дополнительно видят доступное им состояние серверов. Для оплаченных подписок предусмотрены напоминания за три дня, один день и 15 минут, а также обработка окончания срока.

Администратор может:

- проверять uptime, RAM, disk, DNS, UFW, Docker и TLS;
- выполнять разрешённые Docker `inspect` и ограниченный tail логов;
- просматривать fail2ban и получать ежедневную выжимку;
- управлять доступом, пользователями и персональными ссылками;
- рассматривать заявки на тест и оплату;
- отправлять личные сообщения и рассылки;
- начиная с должности инженера сопровождения планировать, продлевать, отменять и завершать техработы;
- брать, передавать, закрывать и архивировать тикеты;
- использовать RemnaWave `/metrics` в режиме `BOT_MODE=mixed`.

## Доступ и роли

- `/auth <ADMIN_PASSWORD>` авторизует администратора. Сообщение с паролем бот пытается сразу удалить.
- `/owner <OWNER_PASSWORD>` однократно назначает единственного руководителя сервиса.
- Обычный пользователь отправляет заявку, которую администратор одобряет, отклоняет или блокирует.
- Доступ привязан к неизменяемому Telegram ID; username используется только для отображения.
- `/logout` отключает доступ, но не удаляет учётную запись и не снимает блокировку.
- Ограничение попыток входа действует одновременно на конкретного отправителя и глобально.

Уровни сервиса:

- `basic` — базовый доступ, тикеты, тест и покупка подписки;
- `subscriber` — оплаченный доступ и просмотр состояния серверов;
- `unlimited_trial` — доступ уровня подписчика без оплаты и срока окончания.

Должности сотрудников: специалист поддержки, инженер сопровождения, ведущий инженер сопровождения и руководитель сервиса. Управлять активными и запланированными техработами могут сотрудники начиная с инженера сопровождения; специалисту поддержки этот раздел недоступен. Административная роль не заменяет VPN-подписку; исключение — единственный руководитель, которому назначается бессрочный доступ.

## Архитектура

Код организован по функциям продукта. Общих каталогов `handlers/` и `services/` больше нет: обработчики, правила, представления и интеграции находятся рядом со своей предметной областью.

```text
app/
  access/                  вход, заявки на доступ, защита от перебора
  administration/          настройки сервиса и профили сотрудников
  bot/                     композиция PTB, маршруты, jobs, меню и UI
  config/                  Pydantic-настройки, секреты и preflight-проверки
  maintenance/             активные и запланированные техработы
  messaging/               outbox, FloodWait и очистка сообщений
  monitoring/
    docker/                локальные Docker-адаптеры и callbacks
    fail2ban/              чтение, cursor, parser, digest и callbacks
    remote/                SSH transport и удалённые адаптеры
    remnawave/             HTTP-клиент, Prometheus parser и модели
    status/                сбор, cache, представление и диагностика
    system/                локальные DNS, metrics, process и UFW
    tls/                   проверка сертификатов, состояние и jobs
  persistence/
    migration/             явная миграция точной монолитной схемы v4
    repositories/          репозитории отдельных хранилищ
    backend.py             split JSON backend
    unit_of_work.py        общая транзакция нескольких репозиториев
    transaction.py         журнал и атомарная публикация изменений
  runtime/                 logging, process runner и process lock
  subscriptions/
    requests/              заявки, оплата, review и lifecycle
  tickets/                 пользовательские и административные тикеты
  users/
    admin/                 управление пользователями и рассылки
  config_check.py          production preflight CLI
  launcher.py              безопасная точка запуска
  main.py                  тонкий API входа в Telegram-приложение
  storage.py               прикладная граница persistence
```

Архитектурные тесты запрещают возвращать общие `handlers/` и `services/`, ограничивают любой Python-модуль 400 строками и удерживают корень `app/` тонким.

`app.main` только делегирует сборку в `app.bot`. `app.launcher` настраивает logging, получает process lock, открывает хранилище и лишь затем импортирует и запускает Telegram-приложение. Импорт persistence-модулей сам по себе не читает и не изменяет данные.

## Хранение данных

Текущий формат — `split-layout v1`. `storage_layout.json` содержит общую ревизию, пути и SHA-256 каждого store. Изменения нескольких store публикуются одной транзакцией через журнал, `fsync`, атомарную замену и межпроцессную блокировку.

| Область | Файл |
|---|---|
| Профили пользователей | `data/users/profiles.json` |
| Роли и состояние доступа | `data/access/grants.json` |
| Подписки и ссылки | `data/subscriptions/accounts.json` |
| Заявки на услуги | `data/subscriptions/requests.json` |
| Платёжные настройки | `data/subscriptions/billing_settings.json` |
| Помощь и контакты | `data/settings/help_and_contacts.json` |
| Тикеты | `data/support/tickets.json` |
| История сообщений тикетов | `data/support/ticket_messages.json` |
| Техработы | `data/maintenance/state.json` |
| Очередь доставки | `data/messaging/outbox.json` |
| Аудит | `data/audit/events.json` |
| DNS cache | `data/monitoring/dns_cache.json` |
| Status cache | `data/monitoring/node_status_cache.json` |
| Docker cache | `data/monitoring/docker_cache.json` |
| TLS state | `data/monitoring/tls_state.json` |
| Курсоры fail2ban | `data/monitoring/fail2ban_cursors.json` |

Незавершённые Telegram-диалоги хранятся отдельно в `data/telegram/persistence.pickle`, process-local state lock — в `data/runtime/state.lock`.

Разделение по предметным данным и интерфейс `UnitOfWork` являются заделом для будущего PostgreSQL backend: Telegram-сценарии не зависят от имён старых монолитных файлов. Реализация PostgreSQL пока не входит в проект.

Не редактируйте JSON во время работы бота и не удаляйте lock-файлы активного процесса.

## Новая установка

Нужен Python 3.10 или новее.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r app/requirements.txt
cp app/.env.example app/.env
cp app/env.secrets.example app/env.secrets
```

В `app/env.secrets` обязательны:

```env
BOT_TOKEN=123456:telegram-token
ADMIN_PASSWORD=длинный-сгенерированный-пароль
OWNER_PASSWORD=другой-длинный-сгенерированный-пароль
```

Оба пароля должны содержать не менее 16 символов и отличаться друг от друга.

Для совершенно новой установки создайте пустой layout явной командой:

```bash
.venv/bin/python -m app.persistence.bootstrap --data-dir data
```

Команда идемпотентна, создаёт все 16 store и каталог Telegram persistence. Если в каталоге обнаружены `user_data.json` или `important_data.json`, пустая инициализация отказывается продолжать и направляет к мигратору.

Проверьте окружение и запустите бот:

```bash
.venv/bin/python -m app.config_check
.venv/bin/python -m app.launcher
```

Установленный пакет также предоставляет команду `maintbot`.

## Миграция монолитной схемы v4

Миграция никогда не выполняется при импорте или обычном запуске. Поддерживается только точная схема v4 из пары `user_data.json` и `important_data.json`; другая версия, неизвестные поля, дубли ключей, частичный layout или несколько руководителей приводят к отказу без публикации данных.

Перед миграцией остановите сервис. Сначала выполните строго read-only проверку:

```bash
.venv/bin/python -m app.persistence.migration \
  --data-dir /opt/maintbot/data \
  --dry-run
```

Затем укажите каталог резервных копий вне `DATA_DIR`:

```bash
.venv/bin/python -m app.persistence.migration \
  --data-dir /opt/maintbot/data \
  --backup-root /var/backups/maintbot
```

Мигратор:

1. строго валидирует оба JSON;
2. создаёт полную резервную копию с manifest, размерами и SHA-256;
3. повторно проверяет, что источник не изменился;
4. распределяет данные по 16 store без потери неизвестных значений;
5. копирует PTB persistence побайтно;
6. атомарно публикует layout и проверяет результат.

После миграции:

```bash
.venv/bin/python -m app.persistence.migration --data-dir /opt/maintbot/data --dry-run
.venv/bin/python -m app.config_check
```

Повторный dry-run должен вернуть `already_migrated=true`. Только после проверки и пробного запуска перенесите старые `user_data.json`, `important_data.json`, `ptb_persistence`, их `.lock` и `important_data.fail2ban_state.*.json` из рабочего `DATA_DIR` в закрытый архив. Не удаляйте checksum-проверенную резервную копию.

## Настройка окружения

Полный перечень и безопасные значения приведены в `app/.env.example`. Основные группы:

- пути: `DATA_DIR`, `PTB_PERSISTENCE_PATH`, `INSTANCE_LOCK_PATH`;
- защита входа: `AUTH_*`, `ACCESS_REQUEST_COOLDOWN_SEC`;
- доставка: `OUTBOX_PROCESS_INTERVAL_SEC`, `ERROR_NOTIFY_INTERVAL_SEC`;
- очистка чатов: `MESSAGE_CLEANUP_*`;
- локальный сервер: `LOCAL_SERVER_*`, `MONITOR_CONTAINERS`, `EXPECTED_A_IP`, `CHECK_A_DOMAINS`;
- DNS и fail2ban: `DNS_*`, `FAIL2BAN_*`;
- SSH: `SSH_IDENTITY_FILE`, `SSH_KNOWN_HOSTS_FILE`, `SSH_STRICT_HOST_KEY_CHECKING=yes`;
- удалённые серверы: `REMOTE_SERVER_*`;
- RemnaWave: `BOT_MODE`, `REMNAWAVE_*`, UUID нод;
- лимиты subprocess, HTTP-ответов и cache.

Все непустые plural-поля удалённых серверов должны соответствовать порядку и количеству `REMOTE_SERVER_SSH_TARGETS`. Группы доменов и контейнеров разделяются `;`, элементы внутри группы — `,`. IPv6 с портом записывается как `maintbot@[2001:db8::1]:22`.

Для нестандартных env-файлов задайте `ENV_PATH` и `SECRETS_ENV_PATH` в окружении процесса до запуска Python.

## Production-развёртывание

Код и venv должны принадлежать root, `data/` — отдельному сервисному пользователю.

```bash
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin maintbot
sudo install -d -o root -g root -m 0755 /opt/maintbot
sudo install -d -o maintbot -g maintbot -m 0700 /opt/maintbot/data
sudo python3 -m venv /opt/maintbot/.venv
sudo /opt/maintbot/.venv/bin/python -m pip install -r /opt/maintbot/app/requirements.txt
```

Env-файлы должны быть доступны root и группе сервиса, но не остальным пользователям:

```bash
sudo chown root:maintbot /opt/maintbot/app/.env /opt/maintbot/app/env.secrets
sudo chmod 0640 /opt/maintbot/app/.env /opt/maintbot/app/env.secrets
```

### Привилегированный helper

Не добавляйте `maintbot` в общую sudo-группу и не выдавайте `NOPASSWD: ALL`.

```bash
sudo install -d -o root -g root -m 0755 /usr/local/libexec /etc/maintbot
sudo install -o root -g root -m 0755 \
  /opt/maintbot/deploy/maintbot-helper /usr/local/libexec/maintbot-helper
sudo install -o root -g root -m 0644 \
  /opt/maintbot/deploy/fail2ban-paths.example /etc/maintbot/fail2ban-paths
sudo install -o root -g root -m 0440 \
  /opt/maintbot/deploy/maintbot-sudoers /etc/sudoers.d/maintbot
sudo visudo -cf /etc/sudoers.d/maintbot
```

Helper проверяет каждое действие и разрешает только read-only UFW, Docker и ограниченное чтение настроенных fail2ban-логов. Такой же helper нужен на SSH-серверах, где используются эти проверки.

### SSH

Используйте отдельный ключ и заранее проверенный `known_hosts`.

```bash
sudo install -d -o root -g maintbot -m 0750 /etc/maintbot/ssh
sudo ssh-keygen -t ed25519 -N '' -C maintbot -f /etc/maintbot/ssh/id_ed25519
sudo chown maintbot:maintbot /etc/maintbot/ssh/id_ed25519
sudo chmod 0600 /etc/maintbot/ssh/id_ed25519
```

Проверяйте fingerprint host key через доверенный канал. В production обязательны `SSH_STRICT_HOST_KEY_CHECKING=yes`, явные `SSH_IDENTITY_FILE` и `SSH_KNOWN_HOSTS_FILE`.

Боту не нужен входящий Telegram-порт: polling использует исходящий HTTPS. Для удалённых серверов разрешайте SSH только с IP хоста бота.

### systemd

```bash
sudo cp /opt/maintbot/deploy/maintbot.service /etc/systemd/system/maintbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now maintbot.service
```

Unit запускает `app.config_check` до приложения, предоставляет запись только в `/opt/maintbot/data`, использует `ProtectSystem=strict`, private tmp/devices, `UMask=0077` и лимиты ресурсов. Второй экземпляр завершается с кодом 75 и не запускает Telegram polling.

Проверка:

```bash
sudo -u maintbot /opt/maintbot/.venv/bin/python -m app.config_check
sudo systemd-analyze verify /etc/systemd/system/maintbot.service
sudo systemctl status maintbot.service --no-pager -l
sudo journalctl -u maintbot.service -n 200 --no-pager
```

## Надёжность доставки

Уведомление сначала фиксируется в persisted outbox одной транзакцией с изменением состояния, а затем отправляется в Telegram. Временные ошибки повторяются с backoff и общим FloodWait gate. Семантика доставки — «как минимум один раз»: редкий сбой после отправки, но до фиксации результата может создать дубль, однако не должен молча потерять событие.

Курсор fail2ban сдвигается после завершения доставки. Учитываются inode/device, copytruncate и ротация `.1`; чтение ограничено по строкам и байтам.

ID сообщений личных чатов сохраняются в PTB persistence. Периодическая очистка удаляет старые сообщения в пределах ограничений Telegram и всегда оставляет последнее сообщение диалога.

## Фоновые задачи

- обработка persisted outbox;
- ежедневный fail2ban digest;
- startup и ежедневное обновление DNS;
- кеширование Docker и состояния серверов;
- проверка TLS-сертификатов и уведомления об истечении;
- запланированные техработы и напоминания;
- очистка лимитов авторизации;
- освобождение осиротевших тикетов;
- lifecycle подписок, заявок и напоминаний;
- очистка сообщений личных чатов.

## Telegram-команды

- `/start`, `/menu`, `/help`;
- `/auth <пароль>`, `/login <пароль>`;
- `/owner <пароль>`;
- `/logout`;
- `/health`, `/subscription`, `/ticket`;
- `/users`, `/fail2ban` — для администратора;
- `/maint` — для инженера сопровождения, ведущего инженера или руководителя сервиса;
- `/cancel` — выход из текущего диалога.

## Разработка и проверки

```bash
.venv/bin/python -m pip install -r app/requirements-dev.txt
.venv/bin/python -m ruff format --check app tests
.venv/bin/python -m ruff check app tests
.venv/bin/python -m mypy app
.venv/bin/python -m pytest -q
.venv/bin/python -m bandit -r app -q
.venv/bin/python -m vulture app --min-confidence 90 --ignore-names cls
.venv/bin/python -m compileall -q app tests
.venv/bin/python -m pip check
.venv/bin/python -m pip_audit -r app/requirements.txt
```

`pyproject.toml`, тесты и `app/requirements-dev.txt` должны находиться под контролем Git. `data/`, env-файлы, IDE/cache/build-артефакты и локальная `docs/` исключены через `.gitignore`.

## Обновление и откат

Перед обновлением:

1. остановите сервис;
2. скопируйте весь `DATA_DIR` в закрытый каталог вне рабочей директории;
3. замените root-owned код;
4. обновите venv по `app/requirements.txt`;
5. выполните требуемую явную миграцию;
6. запустите `app.config_check`;
7. только затем запускайте сервис.

Для отката остановите сервис и восстановите одновременно совместимые версии кода, venv и полного каталога данных. Не запускайте старую и новую версии с одним `DATA_DIR`.

## Границы безопасности

- Содержимое `data/`, env-файлы, SSH-ключи, Telegram file ID и тексты тикетов являются приватными.
- Не публикуйте production-логи без очистки ID, username, адресов и обращений.
- Docker daemon и sudo helper остаются чувствительными даже при read-only интерфейсе.
- Локальное окончание подписки ограничивает функции MaintBot, но без отдельной интеграции не отключает пользователя в RemnaWave.
- Никогда не коммитьте production-данные или секреты.
