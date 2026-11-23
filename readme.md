# ChGK Results

Загрузка результатов спортивного «Что? Где? Когда?» из API рейтинга в Postgres. Приложения и исследования читают SQL. Пишет в базу только этот загрузчик.

Старый файловый загрузчик лежит в [`legacy/`](legacy/).

Спецификация таблиц — в [`task.md`](task.md). Схема — в [`schema.sql`](schema.sql).

## Конфиг

`config.example.json` — локальный прогон, `write_sql` выключен. `config.prod.json` — прод: запись включена, окно 90 дней.

Секреты — в `secrets.yaml` рядом с конфигом. Файл в `.gitignore`, на машину его копируют отдельно. Загрузчик подключается как `chgk_owner`. Снаружи ходят как `chgk_reader`, у этой роли только `SELECT`.

```yaml
api_token: rt_...
database_url: postgresql://chgk_owner:ПАРОЛЬ@127.0.0.1:5434/chgk
```

Токен с режимом «Публичный». `database_url` обязателен, только если `write_sql` включён. Если адрес базы указан, а запись выключена, загрузчик может читать базу, чтобы не затереть уже открытую маску, но не пишет.

Окно по умолчанию — 90 дней. Пауза `0.12` с держит чтение ниже лимита токена 600 запросов в минуту. Воркеры качают параллельно, запись каждого турнира идёт своим подключением.

## Команды

Из папки `chgk_results`. Своё окружение в `.venv`, общий `/root/python/.venv` не трогаем:

```bash
deactivate 2>/dev/null || true
rm -rf .venv
python3 -m venv .venv
# или явно 3.10: ~/.pyenv/versions/3.10.12/bin/python -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements.txt

python chgk_results.py load-one --id 5022
python chgk_results.py update
python chgk_results.py load-days --days 730
python chgk_results.py load-all
```

- `load-one` — один турнир.
- `update` — турниры с `dateEnd` внутри окна из конфига и турниры с `lastEditDate` новее последней успешной заливки.
- `load-days --days N` — заново скачать турниры, которые закончились за последние N дней. Год или два, без всей истории. Уже залитые в этом окне качаются снова.
- `load-all` — все турниры. Уже залитые с тем же `last_edit_date` пропускаются, после остановки запуск продолжается.

На виртуальной машине `update` запускается в четверг в 05:00:00 UTC. Расписание лежит в [`deploy/chgk-results.cron`](deploy/chgk-results.cron).

## Прод на 45.132.18.2

Код: `/root/python/chgk_results`. Машина — Ubuntu 24.04, системный Python 3.12.3. Его не менять. Старый `/root/python/.venv` собран до обновления ОС и для этого загрузчика не используется; каталог не удалять.

Postgres на машине три кластера. База `chgk` создаётся только в PostgreSQL 16:

- PostgreSQL 12, порт 5432 — старый кластер. Не останавливать и не удалять.
- PostgreSQL 14, порт 5433 — пустой, не трогать.
- PostgreSQL 16, порт 5434 — сюда пишем. `listen_addresses = '*'`, SSL включён, сертификат пока самоподписанный.

Базы `communication`, `iter_template_prod` и `main` уже перенесены на 16. Базы `chgk` среди них не было, её создаём заново. Роли `roman`, `tester`, `watcher` уже есть на 16, их пароли не менять. Новые пароли сервер сохраняет как SCRAM, потому что `password_encryption = scram-sha-256`. Метод `md5` в `pg_hba.conf` принимает и старые MD5-хеши, и SCRAM.

1. С ноутбука скопируйте токен. В локальном `secrets.yaml` есть `api_token`.

```bash
scp /home/roman/python/chgk/chgk_results/secrets.yaml root@45.132.18.2:/root/python/chgk_results/secrets.yaml
```

2. На машине обновите код и соберите отдельное окружение в каталоге репозитория. `rm -rf .venv` ниже удаляет только `/root/python/chgk_results/.venv`. Общий `/root/python/.venv` не удалять. На этой машине `python3` — системный 3.12.3; если нужно 3.10, используйте закомментированную строку с pyenv.

```bash
ssh root@45.132.18.2
cd /root/python/chgk_results
git pull
deactivate 2>/dev/null || true
rm -rf .venv
python3 -m venv .venv
# или явно 3.10: ~/.pyenv/versions/3.10.12/bin/python -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements.txt
```

3. Проверьте кластер 16, ничего в нём не меняя.

```bash
timedatectl
grep -n "^port\|^listen_addresses\|^ssl \|^password_encryption" /etc/postgresql/16/main/postgresql.conf
grep -vE '^[[:space:]]*#' /etc/postgresql/16/main/pg_hba.conf
```

Ожидается `port = 5434`, `listen_addresses = '*'`, `ssl = on`. Расписание само в UTC через `CRON_TZ`. Внешние строки `pg_hba.conf` — это `hostssl` для ролей `roman`, `tester`, `watcher`, метод `md5`, любые IPv4 и IPv6. База `chgk` под них не попадает: в строке указана роль, а не «все». `postgres` и `root` снаружи не открыты, так и оставляем.

4. Создайте роли и базу на порту 5434. Без `-p 5434` команда может попасть в PostgreSQL 12. Пароли свои. `chgk_owner` владеет базой: может выдавать права, удалять таблицы и менять представления внутри неё. Это не суперпользователь.

```bash
sudo -u postgres psql -p 5434 -c "CREATE ROLE chgk_owner LOGIN PASSWORD 'ПАРОЛЬ_OWNER';"
sudo -u postgres psql -p 5434 -c "CREATE ROLE chgk_reader LOGIN PASSWORD 'ПАРОЛЬ_READER';"
sudo -u postgres psql -p 5434 -c "CREATE DATABASE chgk OWNER chgk_owner;"
```

5. Добавьте читателя во внешний доступ кластера 16. В `/etc/postgresql/16/main/pg_hba.conf` допишите те же две строки, что уже есть для `roman`, с ролью `chgk_reader` и базой `chgk`:

```
hostssl chgk chgk_reader 0.0.0.0/0 md5
hostssl chgk chgk_reader ::/0 md5
```

Локальный TCP на `127.0.0.1` оставьте как есть, если он уже разрешён всем ролям. Если локальные строки тоже ограничены списком `roman`, `tester`, `watcher`, добавьте `chgk_owner` и `chgk_reader` только для `127.0.0.1/32` и `::1/128`, тем же методом, что у соседней локальной строки. Затем:

```bash
sudo -u postgres psql -p 5434 -c "SELECT pg_reload_conf();"
```

6. Создайте таблицы от имени владельца и выдайте чтение. Порт в каждой команде — 5434.

```bash
cd /root/python/chgk_results
PGPASSWORD='ПАРОЛЬ_OWNER' psql -h 127.0.0.1 -p 5434 -U chgk_owner -d chgk -f schema.sql
PGPASSWORD='ПАРОЛЬ_OWNER' psql -h 127.0.0.1 -p 5434 -U chgk_owner -d chgk -f roles.sql
```

`roles.sql` забирает у `PUBLIC` доступ к базе `chgk` и оставляет `CONNECT` владельцу и читателю. Читатель получает `SELECT` на текущие и будущие таблицы схемы `chgk`. Другие базы этот файл не изменяет.

7. Включите продовый конфиг.

```bash
cp config.prod.json config.json
chmod 600 /root/python/chgk_results/secrets.yaml
```

В `secrets.yaml` рядом с токеном — адрес владельца на кластере 16:

```yaml
database_url: postgresql://chgk_owner:ПАРОЛЬ_OWNER@127.0.0.1:5434/chgk
```

Снаружи читатель ходит так. `sslmode=require` обязателен: внешние строки `pg_hba` — это `hostssl`, а сертификат самоподписанный, поэтому `verify-full` не подойдёт.

```yaml
postgresql://chgk_reader:ПАРОЛЬ_READER@45.132.18.2:5434/chgk?sslmode=require
```

8. Проверьте, что сессия попала в PostgreSQL 16, и запишите один турнир.

```bash
PGPASSWORD='ПАРОЛЬ_OWNER' psql -h 127.0.0.1 -p 5434 -U chgk_owner -d chgk -c "SHOW server_version;"
.venv/bin/python chgk_results.py load-one --id 5022
PGPASSWORD='ПАРОЛЬ_OWNER' psql -h 127.0.0.1 -p 5434 -U chgk_owner -d chgk -c "select tournament_id, tournament_name from chgk.tournaments;"
```

Версия начинается с `16.`. У загрузчика `записано=1`, в выборке одна строка. Та же выборка на порту 5432 к базе `chgk` должна отказать: этой базы на PostgreSQL 12 нет.

9. Проверьте читателя: свой SELECT проходит, запись нет.

```bash
PGPASSWORD='ПАРОЛЬ_READER' psql -h 127.0.0.1 -p 5434 -U chgk_reader -d chgk -c "select count(*) from chgk.tournaments;"
PGPASSWORD='ПАРОЛЬ_READER' psql -h 127.0.0.1 -p 5434 -U chgk_reader -d chgk -c "delete from chgk.tournaments where tournament_id = 5022;"
```

Второе заканчивается отказом в праве. Снаружи: хост `45.132.18.2`, порт `5434`, пользователь `chgk_reader`, база `chgk`, `sslmode=require`. Запрос `select count(*) from chgk.tournaments` возвращает число.

10. Залейте нужный срок и поставьте расписание.

```bash
.venv/bin/python chgk_results.py load-days --days 730
crontab -l
```

Если у root уже есть задания, откройте `crontab -e` и добавьте две строки из [`deploy/chgk-results.cron`](deploy/chgk-results.cron). Команда `crontab файл` заменила бы весь список.

Четверг, 05:00:00 UTC, команда `update`, интерпретатор `/root/python/chgk_results/.venv/bin/python`. Лог: `/var/log/chgk_results.log`. Вся история за 30 лет — отдельный `.venv/bin/python chgk_results.py load-all`, его можно прервать и запустить снова.

## Таблицы

Схема `chgk`.

- `tournaments` — одна строка на турнир.
- `tournaments_with_teams` — участие команды. `potential` — средняя сложность взятых вопросов основного пакета. `c_star`: 0 — взяты самые лёгкие, 1 — самые трудные; пусто, если взято ничего, всё, или вопросы одной сложности. `flags` — зачёты команды. `player_ids` — id состава.
- `tournaments_with_players` — участие игрока.
- `tournaments_with_questions` — символ маски. `tiebreak_round = 0` — основной пакет, дальше раунды перестрелки. У перестрелки и снятых вопросов `difficulty` пустая, в `potential` и `c_star` они не входят.
- `tournament_load` — служебная таблица продолжения полной загрузки.

Во всех строках есть `inserted_at`.

Дата отыгрыша синхрона и асинхрона берётся из `dateStart` заявки (`GET /tournaments/{id}/requests`) по `synchRequest.id`. Если заявки нет, дата и площадка остаются пустыми, а команда всё равно записывается. У очника дата — это `date_start` турнира.

Если API отдал скрытые результаты, а в базе уже есть маска, замена не выполняется.

Новые строки турнира сначала попадают во временные таблицы. Проверка счётчиков проходит до `delete`. Старые строки этого `tournament_id` снимаются и новые пишутся в той же транзакции.

## Проверки

Сразу после ответа API: это список, у каждой команды есть id, флаги запрошены (`includeTeamFlags=1`), символы маски и перестрелки из известного набора, маски одной длины, у синхрона каждая заявка находится и у неё есть `dateStart`.

Перед записью: ключи уникальны, у вопросов и игроков есть команда, `player_ids` совпадает с составом, `potential` и `c_star` сходятся с формулой, `c_star` внутри 0..1.

Тесты, без Postgres и без сети:

```bash
python3 -m unittest discover -s tests -v
```
