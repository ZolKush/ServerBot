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
- одобрять стандартный тест на 24 часа, отклонять заявки и отправлять реквизиты;
- отправлять личные сообщения и рассылки всем пользователям либо только администраторам;
- выгружать для руководителя XLSX-таблицу клиентов и заявок;
- начиная с должности инженера сопровождения планировать, продлевать, отменять и завершать техработы;
- брать, передавать, закрывать и архивировать тикеты;
- использовать RemnaWave `/metrics` для серверов, которым этот источник назначен в inventory.

## Доступ и роли

- `/auth <ADMIN_PASSWORD>` авторизует администратора. Сообщение с паролем бот пытается сразу удалить.
- `/owner <OWNER_PASSWORD>` однократно назначает единственного руководителя сервиса.
- Обычный пользователь отправляет заявку, которую администратор одобряет, отклоняет или блокирует.
- Доступ привязан к неизменяемому Telegram ID; username используется только для отображения.
- `/logout` отключает текущую сессию, но не удаляет учётную запись и не снимает блокировку. При следующем `/start` ещё действующая оплаченная подписка восстанавливает доступ автоматически; неоплаченный или истёкший доступ снова требует заявки.
- Ограничение попыток входа действует одновременно на конкретного отправителя и глобально.

Уровни сервиса:

- `basic` — базовый доступ, тикеты, тест и покупка подписки;
- `subscriber` — оплаченный доступ и просмотр состояния серверов;
- `unlimited_trial` — доступ уровня подписчика без оплаты и срока окончания.

Должности сотрудников: специалист поддержки, инженер сопровождения, ведущий инженер сопровождения и руководитель сервиса. Управлять активными и запланированными техработами могут сотрудники начиная с инженера сопровождения; специалисту поддержки этот раздел недоступен. Административная роль не заменяет VPN-подписку; исключение — единственный руководитель, которому назначается бессрочный доступ.

Специалист поддержки и остальные администраторы могут одобрить только стандартный тест на 24 часа, отправить реквизиты либо отклонить незавершённую заявку. Только руководитель может выбрать другой срок теста, подтвердить или не найти платёж и зарегистрировать оплату вручную.

Для теста бот всегда запрашивает новую персональную ссылку и сохраняет точное время окончания. Сотрудник обязан создать эту ссылку во внешней панели сразу с тем же сроком: MaintBot скрывает и удаляет её локально после дедлайна, но сам не отзывает доступ в RemnaWave.

## Архитектура

Код организован по функциям продукта. Общих каталогов `handlers/` и `services/` больше нет: обработчики, правила, представления и интеграции находятся рядом со своей предметной областью.

```text
app/
  access/                  вход, заявки на доступ, защита от перебора
  administration/          настройки сервиса и профили сотрудников
  bot/                     композиция PTB, маршруты, jobs, меню и UI
  config/                  строгие JSON-конфиги, секреты и preflight
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
    admin/                 управление, рассылки и XLSX-выгрузка
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

Нужен Python 3.10 или новее. Проект проверяется также на актуальной поддерживаемой версии Python; зависимости
зафиксированы совместимым набором, а не обновляются автоматически до любого нового major-релиза.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r app/requirements.txt
cp app/env.secrets.example app/env.secrets
mkdir -p data/conf/servers
cp examples/conf/bot.json data/conf/bot.json
cp examples/conf/servers/*.json data/conf/servers/
```

В `app/env.secrets` обязательны:

```env
BOT_TOKEN=123456:telegram-token
ADMIN_PASSWORD=длинный-сгенерированный-пароль
OWNER_PASSWORD=другой-длинный-сгенерированный-пароль
REMNAWAVE_METRICS_USER=необязательно
REMNAWAVE_METRICS_PASS=необязательно
```

Оба пароля должны содержать не менее 16 символов и отличаться друг от друга. Логин и пароль RemnaWave задаются только вместе либо оба остаются пустыми; неизвестные ключи в `env.secrets` считаются ошибкой конфигурации.
Несекретные настройки больше не читаются из `.env`. По умолчанию бот открывает `data/conf/bot.json` и сканирует
все непосредственные файлы `data/conf/servers/*.json`. Для нестандартного расположения задаётся только bootstrap-
переменная `MAINTBOT_CONFIG_DIR`; секретный файл по-прежнему можно перенести через `SECRETS_ENV_PATH`.

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

## Конфигурация

`data/conf/bot.json` содержит полный набор несекретных настроек процесса: пути, расписания, лимиты, SSH,
RemnaWave endpoint, retry/cache intervals, очистку панелей и защиту входа. Документ имеет `version: 1`; пропущенный,
лишний или дублирующийся ключ, неверный тип, неизвестная timezone, некорректный URL/IP либо повреждённый JSON
останавливает запуск. Логин и пароль RemnaWave, Telegram token и пароли ролей остаются только в
`app/env.secrets` или переменных окружения процесса.

Каждый непосредственный файл `data/conf/servers/*.json` описывает ровно один сервер целиком: `key`,
`display_order`, подпись/флаг, transport и SSH target, monitoring source/UUID, ожидаемый IP, домены и TLS-порты,
Docker-контейнеры, fail2ban path/timezone. Имена файлов произвольны и не используются как server key. При старте
каталог сортируется и читается как единый snapshot; повреждение любого JSON, повторный `key`, более одного local-
сервера или изменение каталога во время чтения приводит к отказу без частично загруженного inventory. Поддерживается
до 256 файлов. Добавление, удаление или изменение сервера применяется после рестарта бота.

Порядок интерфейса задаёт `display_order`, затем `key`. Для каждого домена отдельно задаются `tls_primary_port` и
`tls_fallback_ports`. Fallback используется только после сетевой/TLS-handshake ошибки основного порта и не маскирует
полученный просроченный, недоверенный или не соответствующий hostname сертификат.

При удалении сервера либо изменении его transport/host/UUID/domains/cache-sensitive настроек startup reconciliation
удаляет несовместимые DNS, node, Docker, TLS и fail2ban projections. Старые данные не показываются новому серверу,
которому повторно присвоили прежний `key`.

### Миграция прежних `.env` + `servers.toml`

Остановите сервис и сделайте закрытую резервную копию `DATA_DIR`, `.env`, `env.secrets` и `servers.toml`. Затем:

```bash
.venv/bin/python tools/migrate_config_layout.py \
  --env app/.env \
  --inventory app/servers.toml \
  --output-dir data/conf

.venv/bin/python -m app.config_check
```

Мигратор не меняет источники, не копирует секреты, отказывается перезаписывать `data/conf`, пишет файлы с закрытыми
правами во временный каталог, выполняет `fsync` и публикует весь каталог атомарно. Старые
`migrate_server_inventory.py` и `migrate_runtime_env.py` сохранены только как первый этап обновления ещё более старой
позиционной v4-конфигурации; их результат затем обязательно переводится командой выше.

Для другого каталога конфигурации экспортируйте `MAINTBOT_CONFIG_DIR`; для другого secret-файла —
`SECRETS_ENV_PATH`. Относительные bootstrap-пути разрешаются от корня проекта, а не от случайного текущего каталога.
Остальные несекретные переменные окружения намеренно не переопределяют `bot.json`.

## Production-развёртывание

Код и venv должны принадлежать root, изменяемая часть `data/` — отдельному сервисному пользователю. Подкаталог
`data/conf` лучше оставить root-owned и доступным сервису только для чтения.

```bash
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin maintbot
sudo install -d -o root -g root -m 0755 /opt/maintbot
sudo install -d -o maintbot -g maintbot -m 0700 /opt/maintbot/data
sudo install -d -o root -g maintbot -m 0750 /etc/maintbot
sudo python3 -m venv /opt/maintbot/.venv
sudo /opt/maintbot/.venv/bin/python -m pip install -r /opt/maintbot/app/requirements.txt
```

Установите JSON-конфигурацию и secret-файл без доступа для остальных пользователей:

```bash
sudo install -d -o root -g maintbot -m 0750 \
  /opt/maintbot/data/conf /opt/maintbot/data/conf/servers
sudo install -o root -g maintbot -m 0640 \
  /opt/maintbot/examples/conf/bot.json /opt/maintbot/data/conf/bot.json
sudo install -o root -g maintbot -m 0640 \
  /opt/maintbot/examples/conf/servers/*.json /opt/maintbot/data/conf/servers/
sudo chown root:maintbot /opt/maintbot/app/env.secrets
sudo chmod 0640 /opt/maintbot/app/env.secrets
```

### Привилегированный helper

Не добавляйте `maintbot` в общую sudo-группу и не выдавайте `NOPASSWD: ALL`.

```bash
sudo install -d -o root -g root -m 0755 /usr/local/libexec /etc/maintbot
sudo install -o root -g root -m 0755 \
  /opt/maintbot/deploy/maintbot-helper /usr/local/libexec/maintbot-helper
sudo install -o root -g root -m 0644 \
  /opt/maintbot/deploy/fail2ban-paths.example /etc/maintbot/fail2ban-paths
sudo install -o root -g root -m 0644 \
  /opt/maintbot/deploy/docker-containers.example /etc/maintbot/docker-containers
sudo install -o root -g root -m 0440 \
  /opt/maintbot/deploy/maintbot-sudoers /etc/sudoers.d/maintbot
sudo visudo -cf /etc/sudoers.d/maintbot
```

Helper проверяет каждое действие и разрешает только read-only UFW, Docker и ограниченное чтение настроенных
fail2ban-логов. Docker `ps`, `inspect` и `logs` доступны только для точных имён из root-owned
`/etc/maintbot/docker-containers`; `inspect` возвращает ограниченный набор полей без `Config.Env`, mounts и labels.
Обновляйте allowlist вместе с `docker.containers` в JSON сервера. Не добавляйте локального или SSH-пользователя бота
в группу `docker`: это обошло бы ограничения helper. Такой же helper и policy-файлы нужны на SSH-серверах, где
используются эти проверки.

### SSH

Используйте отдельный ключ и заранее проверенный `known_hosts`.

```bash
sudo install -d -o root -g maintbot -m 0750 /etc/maintbot/ssh
sudo ssh-keygen -t ed25519 -N '' -C maintbot -f /etc/maintbot/ssh/id_ed25519
sudo chown maintbot:maintbot /etc/maintbot/ssh/id_ed25519
sudo chmod 0600 /etc/maintbot/ssh/id_ed25519
sudo install -o root -g maintbot -m 0640 \
  /root/verified-maintbot-known_hosts /etc/maintbot/ssh/known_hosts
```

Сначала сформируйте `/root/verified-maintbot-known_hosts` и проверьте fingerprint каждого host key через доверенный
канал; не копируйте непроверенный результат `ssh-keyscan` вслепую. В production обязательны
`SSH_STRICT_HOST_KEY_CHECKING=yes`, явные `SSH_IDENTITY_FILE` и `SSH_KNOWN_HOSTS_FILE`.

Боту не нужен входящий Telegram-порт: polling использует исходящий HTTPS. Для удалённых серверов разрешайте SSH только с IP хоста бота.

### systemd

```bash
sudo cp /opt/maintbot/deploy/maintbot.service /etc/systemd/system/maintbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now maintbot.service
```

Unit запускает `app.config_check` через `ExecStartPre` до приложения, поэтому повреждённая конфигурация переводит
unit в состояние failed, а не выглядит как успешно пропущенный запуск. Unit предоставляет запись только в
`/opt/maintbot/data` (конфигурационный подкаталог остаётся read-only), использует `ProtectSystem=strict`, private
tmp/devices, `UMask=0077` и лимиты ресурсов. Второй экземпляр завершается с кодом 75 и не запускает Telegram polling.

Проверка:

```bash
sudo -u maintbot /opt/maintbot/.venv/bin/python -m app.config_check
sudo systemd-analyze verify /etc/systemd/system/maintbot.service
sudo systemctl status maintbot.service --no-pager -l
sudo journalctl -u maintbot.service -n 200 --no-pager
```

## Надёжность доставки

Уведомление сначала фиксируется в persisted outbox одной транзакцией с изменением состояния, а затем отправляется в Telegram. Временные ошибки повторяются с backoff и общим FloodWait gate. Семантика доставки — «как минимум один раз»: редкий сбой после отправки, но до фиксации результата может создать дубль, однако не должен молча потерять событие.

Временная сетевая ошибка активно повторяется не более 72 раз и не дольше семи дней. После этого адресат остаётся
в persisted `dead_letter`: событие не удаляется и не создаёт retry-шторм. После устранения причины остановите сервис,
сделайте backup `DATA_DIR` и явно верните событие в очередь:

```bash
.venv/bin/python -m app.messaging.outbox_redrive \
  --data-dir /opt/maintbot/data --source user --event-id <event-id>
```

Для review-карточек координаты уже отправленного Telegram-сообщения фиксируются до регистрации ссылки. Ошибка
storage/restart повторяет только регистрацию, не вторую отправку карточки.

Курсор fail2ban сдвигается после завершения доставки. Учитываются inode/device, copytruncate и ротация `.1`; чтение ограничено по строкам и байтам.

В PTB persistence сохраняются только явно зарегистрированные навигационные панели бота, включая временные экраны подтверждения и ввода. При старте они удаляются, чтобы старые inline-меню не конфликтовали с новым процессом; периодическая очистка удаляет устаревшие панели и сохраняет последнюю актуальную. Рассылки, уведомления, содержимое и переписка тикетов, пользовательские сообщения и произвольные ответы бота в этот реестр не попадают и не удаляются.

PTB persistence публикуется через временный файл, `fsync` и атомарный `replace`; перед заменой сохраняется последний
валидный образ `.bak`. Если основной pickle оборван из-за `SIGKILL`, нехватки места или сбоя записи, следующий старт
восстанавливает его из backup. Транзакции split JSON сначала полностью проверяют staged-файлы, затем публикуют их;
корректный pending journal допускается preflight-проверкой и завершается launcher recovery до запуска Telegram.

Карточки заявок сохраняют координаты доставленных сообщений у каждого администратора. После решения, смены этапа, отмены ввода или освобождения зависшей заявки бот обновляет все сохранённые карточки до одного текущего статуса; недоступные или удалённые сообщения безопасно исключаются из реестра.

## Резервное удаление сообщений через консоль

`tools/emergency_delete_recent.py` — ручная аварийная утилита, не часть запуска бота или фоновых задач.
Запускается из корня checkout как `python -m tools.emergency_delete_recent`; установка wheel без `tools/`
для этой команды недостаточна. Прежний экспериментальный модуль `app.messaging.emergency_delete_recent` удалён.
Консольные команды с `python - <<'PY'` выполняли собственный код и этот модуль не использовали.

Утилита применяет **явно заданный включительный диапазон message ID** ко всем сохранённым личным чатам
(`--all-chats`) или выбранным `--chat-id` (параметр можно повторять). Она не отправляет точки, не читает
pickle и не пытается вычислять «последние два сообщения» через `message_id - 1`. Соседние номера не гарантируют
соседние сообщения в одном чате. Диапазон задаётся для конкретного инцидента; постоянного рабочего диапазона нет.
Завершённые рассылки сейчас не сохраняют полный журнал координат отправленных сообщений, поэтому это резервный
перебор кандидатов, а не точечный отзыв рассылки по её тексту или времени.

В диапазоне могут быть удалены **любые доступные для удаления сообщения, включая входящие сообщения пользователя**.
Отбор по автору/дате не выполняется. Ограничения Telegram остаются в силе.
[Telegram deleteMessages](https://core.telegram.org/bots/api#deletemessages) пропускает отсутствующие ID:
`ACCEPTED` и `accepted_ids` означают подтверждённый запрос, а не число реально удалённых сообщений.
Даже успешный итог требует проверки нужной рассылки в Telegram.

Пример для отдельного инцидента с диапазоном **8300–8600** (не универсальные значения):

```bash
sudo systemctl stop maintbot.service
# Если INSTANCE_LOCK_PATH находится в /run/maintbot: systemd мог удалить каталог после stop.
sudo install -d -o maintbot -g maintbot -m 0700 /run/maintbot
cd /opt/maintbot

# План без запросов к Telegram; это также режим по умолчанию.
sudo -u maintbot /opt/maintbot/.venv/bin/python -m tools.emergency_delete_recent \
  --all-chats --min-message-id 8300 --max-message-id 8600 --dry-run

# Выполнение; каждый запуск использует новое имя отчёта.
sudo -u maintbot /opt/maintbot/.venv/bin/python -m tools.emergency_delete_recent \
  --all-chats --min-message-id 8300 --max-message-id 8600 --execute \
  --report /opt/maintbot/data/emergency-delete-incident-1.jsonl
```

Оба режима берут штатный process lock до открытия split storage. Dry-run не подключается к Telegram и не отменяет
outbox, но штатное открытие хранилища может восстановить незавершённую локальную транзакцию. Конфигурация и секреты
загружаются обычным способом; нестандартные `MAINTBOT_CONFIG_DIR` / `SECRETS_ENV_PATH` передайте через `sudo -u maintbot env`.
Команда `--help` не требует конфигурации или секретов. При другом `INSTANCE_LOCK_PATH` подготовьте именно его каталог.

По умолчанию идут последовательные пакеты до 100 ID, до пяти попыток на запрос, таймаут чтения 60 секунд.
Параметры: `--batch-size 1..100`, `--attempts N`, `--read-timeout SECONDS`. `REQUEST`, `RETRY` и `WAIT` сразу
показывают прогресс; длительный FloodWait сопровождается сообщением каждые 30 секунд. После исчерпания повторов
диапазон получает `UNRESOLVED`, затем обрабатываются следующие пакеты и чаты. После сетевого таймаута результат
может быть неизвестен, даже если Telegram уже выполнил запрос.

Пакет с `BadRequest` делится до отдельных ID: отказ для одного сообщения не останавливает остальные.
`FORBIDDEN` фиксируется по ответу на удаление, после чего обработка продолжается со следующим чатом.
Нет проверки доступности через отправку сообщения. `CHAT_RESULT` и `SUMMARY` показывают принятые, отклонённые
и неподтверждённые ID, а также список `incomplete_chat_ids`. JSONL-отчёт записывается с flush после каждой строки,
создаётся с правами 0600 на Linux и не перезаписывает существующий файл. Он содержит ID чатов — храните его приватно.

Коды выхода: `0` — план показан или все запросы приняты (фактическое число удалений неизвестно), `1` — ошибка либо
неполный результат, `2` — неверные аргументы, `75` — занят process lock, `130` — остановка через Ctrl+C.
После прерывания или `UNRESOLVED` повторите тот же диапазон для нужных `--chat-id`, используя новое имя отчёта.
Повтор не создаёт сообщений, уже отсутствующие ID пропускаются Telegram.

Очередь рассылок по умолчанию не меняется. Если аварийная рассылка ещё не закончена, добавьте
`--cancel-pending-broadcasts --execute`: это отменит **все** оставшиеся события `admin_broadcast`, включая другие
рассылки, но сохранит остальные типы outbox. При dry-run будет показано только намерение отмены.
После проверки результата и состояния очереди запустите сервис вручную: `sudo systemctl start maintbot.service`.

## Фоновые задачи

- обработка persisted outbox;
- ежедневный fail2ban digest;
- startup и ежедневное обновление DNS;
- кеширование Docker и состояния серверов;
- сетевой TLS handshake один раз при старте и затем раз в семь дней;
- ежедневная локальная переоценка сохранённых дат TLS без сетевого подключения;
- запланированные техработы и напоминания;
- очистка лимитов авторизации;
- освобождение осиротевших тикетов;
- lifecycle подписок, заявок и напоминаний;
- очистка только зарегистрированных навигационных панелей.

На главном экране статуса показывается краткая сводка; подробности TLS и Docker открываются отдельными административными кнопками, а аварийные элементы выводятся сразу. Единая кнопка «Обновить» принудительно обновляет основные метрики и DNS. Проверка disk/UFW через SSH появляется только при технической ошибке или неполном ответе основного RemnaWave monitoring и повторно проверяет это условие перед подключением.

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

GitHub Actions повторяет обязательные проверки на Python 3.10 и 3.14. Локально используйте SDK проекта:

```bash
.venv/bin/python -m pip install -r app/requirements-dev.txt
.venv/bin/python -m ruff format --check app tests tools
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy app
.venv/bin/python -m pytest -q
.venv/bin/python -m bandit -r app tools -q
.venv/bin/python -m vulture app --min-confidence 90 --ignore-names cls
.venv/bin/python -m compileall -q app tests tools
.venv/bin/python -m pip check
.venv/bin/python -m pip_audit -r app/requirements.txt
```

`pyproject.toml`, тесты, `app/requirements-dev.txt` и обезличенные примеры `examples/conf/` должны находиться под
контролем Git. `data/`, env-файлы, legacy production inventory, SSH-ключи, IDE/cache/build-артефакты и локальная
`docs/` исключены через `.gitignore`. `app/.env.example` и `deploy/servers.toml.example` оставлены только для
двухэтапной миграции старых установок и не являются текущей runtime-конфигурацией.

## Обновление и откат

Перед обновлением:

1. остановите сервис;
2. скопируйте весь `DATA_DIR` (включая `conf`) и secret-файл в закрытый каталог вне рабочей директории;
3. замените root-owned код;
4. обновите venv по `app/requirements.txt`;
5. выполните требуемую явную миграцию;
6. запустите `app.config_check`;
7. только затем запускайте сервис.

Для отката остановите сервис и восстановите одновременно совместимые версии кода, venv и полного каталога данных. Не запускайте старую и новую версии с одним `DATA_DIR`.

## Границы безопасности

- Содержимое `data/`, env-файлы, реальный server inventory, SSH-ключи, Telegram file ID и тексты тикетов являются приватными.
- Известные runtime-токен и пароли централизованно редактируются в text/JSON-логах, включая traceback; это не заменяет очистку остальных персональных данных.
- Не публикуйте production-логи без очистки ID, username, адресов и обращений.
- Docker daemon и sudo helper остаются чувствительными даже при read-only интерфейсе.
- Счётчики защиты `/auth` process-local и обнуляются при рестарте; для нескольких экземпляров или restart-resistant rate limit нужен внешний backend. Штатный deploy намеренно запрещает второй экземпляр process lock-файлом.
- `data/telegram/persistence.pickle` является доверенным локальным файлом: атомарная запись защищает от обрыва, но не делает pickle безопасным для данных, доступных на запись постороннему пользователю.
- Process-tree termination полностью обеспечивается в production Linux через отдельную process group; Windows остаётся средой разработки, где принудительное завершение потомков после уже завершившегося родителя является best effort.
- Локальное окончание подписки или теста ограничивает функции MaintBot и убирает ссылку из бота, но без отдельной интеграции не отключает пользователя в RemnaWave.
- Никогда не коммитьте production-данные или секреты.
