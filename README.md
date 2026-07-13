# MaintBot

MaintBot — Telegram-бот для наблюдения за локальными и удалёнными серверами, выдачи подписок, техработ и поддержки пользователей. Бот работает через long polling, хранит состояние локально и рассчитан на запуск отдельным непривилегированным пользователем через systemd.

## Модель доступа

Общего пользовательского пароля больше нет.

- Администратор входит командой `/auth <ADMIN_PASSWORD>`. Пароль должен быть не короче 16 символов, сообщение с ним бот пытается сразу удалить.
- Новый пользователь нажимает «Запросить доступ». Заявка сохраняется и отправляется действующим администраторам.
- Администратор одобряет, отклоняет или блокирует заявку кнопкой в Telegram.
- Состояния привязаны к неизменяемому Telegram ID. Username используется только для отображения.
- `/logout` отключает доступ, но не удаляет запись. Поэтому повторная заявка или повторный вход не снимают блокировку.
- Администраторская авторизация защищена индивидуальным и глобальным ограничением попыток.

Первого администратора нужно авторизовать длинным сгенерированным паролем. После этого он сможет обрабатывать заявки остальных пользователей.

## Возможности

Пользователь может:

- открыть меню и статус доступных серверов;
- получить назначенную подписку;
- создать один активный тикет с текстом, фото или документом;
- продолжить переписку в тикете;
- получать объявления и уведомления о техработах.

Администратор дополнительно может:

- просматривать локальные и SSH-серверы: uptime, RAM, disk, DNS, UFW и Docker;
- получать Docker `inspect` и ограниченный tail логов только для настроенных контейнеров;
- смотреть fail2ban и получать ежедневные выжимки;
- одобрять и блокировать пользователей, менять оплату, nickname и подписку;
- отправлять личные сообщения и рассылки;
- объявлять, планировать, продлевать, отменять и завершать техработы;
- брать, передавать, закрывать и архивировать тикеты;
- использовать RemnaWave `/metrics` в `BOT_MODE=mixed`.

## Надёжность данных и доставки

Состояние хранится в:

- `data/user_data.json` — пользователи, доступ, подписки и очередь пользовательских уведомлений;
- `data/important_data.json` — тикеты, техработы, DNS/status cache, курсоры fail2ban и очередь системных уведомлений;
- `data/ptb_persistence` — незавершённые Telegram-диалоги;
- runtime lock — блокировка единственного процесса.

Текущая схема JSON — версия 2. Старые записи мигрируются и нормализуются при загрузке. Запись выполняется через отдельный временный файл, `fsync`, атомарную замену и межпроцессную блокировку. Некорректный JSON останавливает `config_check`, чтобы пустое состояние не перезаписало пользователей, баны или тикеты; если повреждение обнаружит сам storage loader при прямом запуске, он дополнительно создаст копию с суффиксом `.corrupt-*`.

Важные сообщения сначала сохраняются в outbox вместе с изменением состояния и только затем отправляются в Telegram. Для временных ошибок применяются повторные попытки, backoff и общий FloodWait. Это даёт доставку не хуже «как минимум один раз»: после редкого аварийного завершения между отправкой и фиксацией результата возможен дубль, но уведомление не теряется без следа.

Курсор fail2ban двигается после завершения доставки и хотя бы одной реально успешной отправки. Недоступный terminal-получатель не вызывает ежедневные дубли у остальных; если сообщение не получил никто, старый cursor сохраняется для будущего catch-up. Учитываются inode/device, copytruncate и ротация `.1`; чтение ограничено по строкам и байтам.

## Структура проекта

```text
app/
  launcher.py              защищённая точка запуска
  config_check.py          проверка production-конфигурации до старта
  main.py                  Telegram handlers и фоновые jobs
  settings.py              Pydantic-настройки и описание серверов
  storage.py               миграции, JSON-locking, atomic write и outbox
  handlers/                пользовательские и администраторские сценарии
  services/                SSH, HTTP, DNS, Docker, UFW, fail2ban и доставка
deploy/
  maintbot.service         усиленный systemd unit
  maintbot-helper          root-owned helper с allowlist действий
  maintbot-sudoers         единственная разрешённая sudo-команда
  fail2ban-paths.example   allowlist читаемых логов
tests/                     тесты миграций, гонок, outbox и сервисов
```

`app/.env`, `app/env.secrets`, `data/` и логи содержат чувствительные данные. Не публикуйте их и не прикладывайте их содержимое к issue или коммиту.

## Требования и зависимости

Нужен Python 3.10 или новее. Для production:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip==26.1.2
.venv/bin/python -m pip install -r app/requirements.txt
```

Готовые Linux wheels `pycares 5.x` рассчитаны на glibc 2.26/2.28 и новее. На более старом дистрибутиве pip попробует сборку из исходников; для production предпочтительнее актуальная ОС, чем добавление компилятора в рабочий образ.

Для разработки:

```bash
.venv/bin/python -m pip install -r app/requirements-dev.txt
```

`app/requirements.txt` содержит проверенный runtime lock: зафиксированы и прямые, и совместимые transitive-зависимости. `idna` и актуальный CA bundle закреплены явно для HTTP/TLS-стека. Метаданные `pyproject.toml` перечисляют только прямые зависимости; после любого изменения lock весь набор проверяется заново.

## Настройка окружения

Скопируйте шаблоны и заполните их вне публичного репозитория:

```bash
cp app/.env.example app/.env
cp app/env.secrets.example app/env.secrets
```

В `app/env.secrets` обязательны только:

```env
BOT_TOKEN=123456:telegram-token
ADMIN_PASSWORD=длинный-сгенерированный-пароль
```

`ADMIN_PASSWORD` предназначен только для администраторов. Не добавляйте `AUTH_PASSWORD`: пользовательский доступ теперь выдаётся по заявкам.

Полный безопасный шаблон находится в `app/.env.example`. Основные группы настроек:

- пути: `USER_DATA_PATH`, `IMPORTANT_DATA_PATH`, `PTB_PERSISTENCE_PATH`, `INSTANCE_LOCK_PATH`;
- защита входа: `AUTH_*`, `ACCESS_REQUEST_COOLDOWN_SEC`;
- доставка: `OUTBOX_PROCESS_INTERVAL_SEC`, `ERROR_NOTIFY_INTERVAL_SEC`;
- локальный сервер: `LOCAL_SERVER_*`, `MONITOR_CONTAINERS`, `EXPECTED_A_IP`, `CHECK_A_DOMAINS`;
- fail2ban: `FAIL2BAN_ENABLED`, `FAIL2BAN_LOG_PATH`, `FAIL2BAN_TIMEZONE`, лимиты и расписание;
- SSH: `SSH_IDENTITY_FILE`, `SSH_KNOWN_HOSTS_FILE`, `SSH_STRICT_HOST_KEY_CHECKING=yes`;
- удалённые серверы: `REMOTE_SERVER_*`;
- mixed mode: `REMNAWAVE_*`, UUID нод и `DAILY_NODE_STATUS_REFRESH_AT`;
- ограничения: `SUBPROC_*`, `REMNAWAVE_METRICS_MAX_BYTES`, `STATUS_CACHE_TTL_SEC`.

Все непустые plural-поля удалённых серверов должны иметь тот же порядок и количество элементов, что и `REMOTE_SERVER_SSH_TARGETS`. Группы доменов и контейнеров разделяются `;`, элементы внутри группы — `,`. IPv6 с портом записывается как `maintbot@[2001:db8::1]:22`.

Если используются другие env-файлы, задайте `ENV_PATH` и `SECRETS_ENV_PATH` в окружении процесса до импорта приложения.

## Безопасное production-развёртывание

Ни код, ни виртуальное окружение не должны принадлежать сервисному пользователю. На запись ему нужен только каталог данных.

```bash
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin maintbot
sudo install -d -o root -g root -m 0755 /opt/maintbot
sudo install -d -o maintbot -g maintbot -m 0700 /opt/maintbot/data
```

После копирования release в `/opt/maintbot`:

```bash
sudo chown -R root:root /opt/maintbot/app /opt/maintbot/deploy
sudo find /opt/maintbot/app /opt/maintbot/deploy -type d -exec chmod 0755 {} +
sudo find /opt/maintbot/app /opt/maintbot/deploy -type f -exec chmod 0644 {} +
sudo chown root:maintbot /opt/maintbot/app/.env /opt/maintbot/app/env.secrets
sudo chmod 0640 /opt/maintbot/app/.env /opt/maintbot/app/env.secrets
sudo chmod 0700 /opt/maintbot/data
sudo chown maintbot:maintbot /opt/maintbot/data
```

Создайте venv от root и установите зависимости. Сервис сможет читать и исполнять его, но не сможет подменить код:

```bash
sudo python3 -m venv /opt/maintbot/.venv
sudo /opt/maintbot/.venv/bin/python -m pip install --upgrade pip==26.1.2
sudo /opt/maintbot/.venv/bin/python -m pip install -r /opt/maintbot/app/requirements.txt
```

### Привилегированный helper

Не добавляйте `maintbot` в группу `sudo` и не выдавайте `NOPASSWD: ALL`. Установите проверяющий аргументы helper:

```bash
sudo install -d -o root -g root -m 0755 /usr/local/libexec
sudo install -o root -g root -m 0755 /opt/maintbot/deploy/maintbot-helper /usr/local/libexec/maintbot-helper
sudo install -d -o root -g root -m 0755 /etc/maintbot
sudo install -o root -g root -m 0644 /opt/maintbot/deploy/fail2ban-paths.example /etc/maintbot/fail2ban-paths
sudo install -o root -g root -m 0440 /opt/maintbot/deploy/maintbot-sudoers /etc/sudoers.d/maintbot
sudo visudo -cf /etc/sudoers.d/maintbot
```

Helper разрешает только:

- `ufw status`;
- `docker ps`, `inspect` и ограниченный `logs --tail`;
- stat/tail/range-read только для fail2ban-путей из `/etc/maintbot/fail2ban-paths` и их ротаций.

Тот же root-owned helper, sudoers и allowlist установите на каждый SSH-сервер, если там нужны Docker/UFW/fail2ban. Рекомендуется везде использовать отдельного пользователя `maintbot`.

### SSH без доверия первому подключению

```bash
sudo install -d -o root -g maintbot -m 0750 /etc/maintbot/ssh
sudo ssh-keygen -t ed25519 -N '' -C maintbot -f /etc/maintbot/ssh/id_ed25519
sudo chown maintbot:maintbot /etc/maintbot/ssh/id_ed25519
sudo chmod 0600 /etc/maintbot/ssh/id_ed25519
```

Публичный ключ добавьте на удалённые серверы. Для `authorized_keys` полезно ограничить источник и отключить лишние SSH-возможности:

```text
from="BOT_PUBLIC_IP",restrict ssh-ed25519 AAAA... maintbot
```

Получите host keys отдельно, проверьте fingerprints через доверенный канал или консоль сервера и только после сравнения установите файл:

```bash
ssh-keyscan -p 22 server-one.example > /tmp/maintbot-known-hosts
ssh-keygen -lf /tmp/maintbot-known-hosts
sudo install -o root -g maintbot -m 0640 /tmp/maintbot-known-hosts /etc/maintbot/ssh/known_hosts
rm /tmp/maintbot-known-hosts
```

Не принимайте результат `ssh-keyscan` без проверки fingerprint. В production обязательны `SSH_STRICT_HOST_KEY_CHECKING=yes`, явный `SSH_IDENTITY_FILE` и явный `SSH_KNOWN_HOSTS_FILE`. Клиент также использует `BatchMode=yes` и `IdentitiesOnly=yes`.

### UFW

Боту не нужен входящий Telegram-порт: polling использует исходящий HTTPS. На удалённых серверах разрешите SSH только с IP хоста бота:

```bash
sudo ufw allow from BOT_PUBLIC_IP to any port 22 proto tcp comment 'MaintBot SSH'
sudo ufw status verbose
```

Если политика исходящего трафика запрещающая, хосту бота нужны DNS, HTTPS к Telegram/RemnaWave и SSH к настроенным серверам. Перед включением или изменением UFW сначала добавьте правило для своего административного SSH, оставьте открытую вторую сессию и проверьте новый вход — иначе можно заблокировать себе сервер.

### systemd

```bash
sudo cp /opt/maintbot/deploy/maintbot.service /etc/systemd/system/maintbot.service
sudo systemctl daemon-reload
sudo systemctl enable maintbot.service
sudo systemctl start maintbot.service
```

Unit запускает `app.config_check` до бота и откажется стартовать при неверных путях, правах, SSH-файлах или небезопасном host-key режиме. Он использует `ProtectSystem=strict`, `ProtectHome=true`, private tmp/devices, read-only код, `ReadWritePaths=/opt/maintbot/data`, `UMask=0077`, лимиты памяти/задач/файлов и завершает всё control group.

`app.launcher` получает lock до импорта storage. Первый процесс продолжает работу, каждый следующий завершается с кодом 75 и не выполняет миграции. systemd считает этот код штатным и не создаёт restart-loop.

Проверка перед первым запуском вручную:

```bash
sudo install -d -o maintbot -g maintbot -m 0700 /run/maintbot
sudo -u maintbot /opt/maintbot/.venv/bin/python -m app.config_check
sudo systemd-analyze verify /etc/systemd/system/maintbot.service
```

## Запуск и наблюдение

Локальный безопасный запуск из корня проекта:

```bash
.venv/bin/python -m app.launcher
```

Если пакет установлен, команда `maintbot` указывает на тот же launcher.

Состояние systemd и логи:

```bash
sudo systemctl status maintbot.service --no-pager -l
sudo journalctl -u maintbot.service -n 200 --no-pager
sudo journalctl -u maintbot.service -f
sudo systemd-analyze security maintbot.service
```

Логи `httpx`, `httpcore` и APScheduler ограничены уровнем WARNING, чтобы периодические служебные сообщения не раздували журнал. `LOG_JSON=true` включает структурированный JSON-формат.

## Фоновые задачи

- обработка persisted outbox;
- ежедневная выжимка fail2ban;
- startup и ежедневное обновление DNS;
- mixed-mode обновление disk/UFW;
- предупреждения и активация запланированных техработ;
- напоминания об активных техработах;
- очистка rate-limit авторизации;
- освобождение тикетов, оставшихся назначенными несуществующему администратору.

DNS-проверки выполняются конкурентно с глобальным пределом, а SSH/status-запросы к одному серверу дедуплицируются и кратко кэшируются. Subprocess и HTTP-ответы имеют таймауты и жёсткие лимиты размера.

## Обновление и откат

Перед обновлением остановите сервис и сделайте резервную копию всего `data/`, включая PTB persistence:

```bash
sudo systemctl stop maintbot.service
sudo cp -a /opt/maintbot/data /opt/maintbot/data.backup-$(date +%Y%m%d-%H%M%S)
```

Затем замените root-owned код, обновите venv по `requirements.txt`, восстановите права, выполните `app.config_check`, проверки ниже и запустите сервис. Не запускайте одновременно старую и новую версию с одним каталогом данных.

Для отката остановите сервис, верните предыдущие код/venv и соответствующую резервную копию `data/`. Резервная копия важна даже при автоматической миграции: старый release может не понимать новую схему.

## Полная проверка

Из корня проекта с dev-зависимостями:

```bash
.venv/bin/python -m ruff format --check app tests
.venv/bin/python -m ruff check app tests
.venv/bin/python -m mypy app
.venv/bin/python -m bandit -r app -q
.venv/bin/python -m vulture app --min-confidence 90
.venv/bin/python -m pytest -q -W error
.venv/bin/python -m compileall -q app tests
.venv/bin/python -m pip check
.venv/bin/python -m pip_audit -r app/requirements.txt
```

На Linux дополнительно:

```bash
sudo visudo -cf /etc/sudoers.d/maintbot
sudo systemd-analyze verify /etc/systemd/system/maintbot.service
sudo -u maintbot /opt/maintbot/.venv/bin/python -m app.config_check
```

## Команды Telegram

- `/start`, `/menu`, `/help`;
- `/auth <пароль>` или `/login <пароль>` — только администратор;
- `/logout`;
- `/health`, `/subscription`, `/ticket`;
- `/users`, `/maint`, `/fail2ban` — администратор;
- `/cancel` — выход из текущего диалога.

## Диагностика

Сервис не стартует после `ExecCondition`:

- запустите `python -m app.config_check` от пользователя `maintbot`;
- проверьте права `0600/0640`, существование data/runtime-каталогов, helper, sudo, SSH key и known_hosts;
- смотрите конкретную причину через `journalctl -u maintbot.service`.

SSH недоступен:

- проверьте UFW, адрес/порт и доступ только с IP бота;
- выполните тест от пользователя `maintbot` с теми же `-i` и `UserKnownHostsFile`;
- не обходите ошибку установкой `StrictHostKeyChecking=no`.

Docker/UFW/fail2ban показывает «н/д»:

- проверьте установку helper и sudoers локально и на нужном remote server;
- проверьте путь в `/etc/maintbot/fail2ban-paths`;
- выполните `sudo -n /usr/local/libexec/maintbot-helper ufw-status` от сервисного пользователя.

Уведомление не пришло сразу:

- проверьте persisted outbox и журнал;
- временные Telegram/FloodWait ошибки повторяются автоматически;
- постоянные ошибки вроде запрета бота пользователем помечаются как terminal и логируются.

## Границы безопасности

- Telegram ID является идентификатором доступа; username не используется для блокировок.
- Содержимое `data/`, `.env`, `env.secrets`, SSH key, Telegram file IDs и тексты тикетов считаются приватными.
- Helper даёт read-only диагностику, но доступ к Docker daemon сам по себе чувствителен; не расширяйте его команды без отдельного аудита.
- Не редактируйте JSON во время работы сервиса и не удаляйте lock-файл активного процесса.
- Никогда не публикуйте production-логи без очистки ID, username, адресов и содержимого обращений.
