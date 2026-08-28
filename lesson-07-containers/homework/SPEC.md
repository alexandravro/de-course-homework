# SPEC.md — специфікація домашнього завдання

Це головний документ ДЗ. Тут описано **що саме** має робити кожен файл, який ви пишете,
і за що нараховуються бали. Логіку Python-скрипта чіпати не треба — він **дано**
(`app/gh_ingest.py`). Ваша робота — **контейнеризувати** його і підняти стек із Postgres.

## Що будуємо

Стек із двох контейнерів, що завантажує одну годину подій **GitHub Archive** у Postgres:

```
        ┌─────────────┐   мережа Docker    ┌──────────────┐
        │  ingestor   │  (по імені сервісу)│   postgres   │
        │ (ваш образ) │ ─────────────────► │ 16-alpine    │
        │ download +  │   PGHOST=postgres  │ healthcheck  │
        │ COPY до БД  │                    │ named volume │
        └─────────────┘                    └──────────────┘
```

Джерело — та сама детермінована година, що й у ДЗ L02/L03:
`https://data.gharchive.org/2024-01-15-14.json.gz` (~138 MB, 267 250 подій).
Скрипт фільтрує до 5 цільових типів, прибирає дублікати та порожні `repo_name`,
і вантажить результат у таблицю `github_events` через `COPY`.

**Контрольне число (checkpoint):** після успішного запуску в `github_events` рівно
**211 466** рядків і **5** різних `event_type`:

| event_type | рядків |
|---|---|
| PushEvent | 165 837 |
| PullRequestEvent | 18 980 |
| IssueCommentEvent | 12 281 |
| WatchEvent | 9 769 |
| IssuesEvent | 4 599 |

---

## Завдання 1 — `Dockerfile` · 30 балів

Образ для `app/gh_ingest.py`. **Один образ, одна стадія.** Кожен рядок має бути
обґрунтований; зайвих не пишемо. Вимоги:

- **базовий образ із фіксованим тегом** — `python:3.12-slim`, не `latest` і не `python:3`
  (3 б);
- **залежності окремим шаром перед кодом**: `COPY app/requirements.txt` і `pip install`
  ідуть **вище** за `COPY` коду, установка — з `--no-cache-dir`. Правка `gh_ingest.py` не
  повинна тягнути переустановку `psycopg` (9 б);
- **non-root**: створіть користувача (напр. `appuser`, uid 1000) і поставте `USER` перед
  `ENTRYPOINT` — процес не має бути root (8 б);
- точка монтування кешу `/cache` має належати цьому користувачу (інакше non-root процес
  не зможе туди писати) (4 б);
- `ENTRYPOINT` в **exec-form** (JSON-масив) запускає `app/gh_ingest.py` (3 б);
- `PYTHONUNBUFFERED=1` — без нього `docker compose logs ingestor` мовчить, поки скрипт
  качає 138 MB (3 б).

**Self-check:**

```bash
docker build -t gh-ingest:latest .            # збирається без помилок
touch app/gh_ingest.py && docker build .      # pip install -> "CACHED" / "Using cache"
docker run --rm --entrypoint whoami gh-ingest:latest    # appuser, не root
```

---

## Завдання 2 — `.dockerignore` · 10 балів

Виключіть із build context усе зайве: `__pycache__/`, `*.pyc`, `.git/`, `*.md`,
`.env`/секрети, локальні дані. Менший контекст → швидший і чистіший build.

**Self-check:** у build-логах розмір context помітно менший за розмір репозиторію.

---

## Завдання 3 — `docker-compose.yml`: сервіс `postgres` · 20 балів

- образ `postgres:16-alpine` (3 б);
- credentials через змінні оточення `POSTGRES_USER` / `POSTGRES_PASSWORD` /
  `POSTGRES_DB` (беруться з `.env`) (5 б);
- **named volume** під каталог даних БД (`/var/lib/postgresql/data`) — щоб дані
  переживали `docker compose down` (5 б);
- **`healthcheck`** через `pg_isready` — це те, на що чекатиме ingestor (7 б).

---

## Завдання 4 — `docker-compose.yml`: сервіс `ingestor` · 25 балів

- будується з вашого `Dockerfile` (`build: .`) (4 б);
- підключається до БД **по імені сервісу**: `PGHOST: postgres` — Docker-networking,
  жодних IP чи `localhost` (8 б);
- стартує **лише після** того, як postgres став healthy:
  `depends_on: { postgres: { condition: service_healthy } }` (8 б);
- named volume під кеш завантаження, змонтований у `/cache` (3 б);
- передає `PGUSER`/`PGPASSWORD`/`PGDATABASE`/`ARCHIVE_URL` через environment (2 б).

---

## Завдання 5 — запуск + скриншоти · 15 балів

Підніміть стек і **прикладіть скриншоти** у `screenshots/` (див. README → «Що здавати»):

1. `docker compose up --build` — фінал логів ingestor із рядком
   `Done. Loaded 211466 rows ...` (5 б);
2. `docker compose ps` (або `docker ps`) зі станом контейнерів (3 б);
3. результат SQL-запиту з розбивкою по `event_type` (psql або будь-який клієнт) — має
   збігатися з таблицею checkpoint вище (7 б).

---

## Бонус · +10 балів — якість образу

- фінальний образ ≲ 250 MB — тобто в ньому немає ні pip-кешу, ні зайвих файлів із
  build context (5 б);
- доданий робочий `HEALTHCHECK` у Dockerfile (5 б).

---

**Definition of done:** `./verify.sh` (з кореня `homework/`) зелений — стек піднявся,
ingestor завершився кодом 0, у Postgres рівно 211 466 рядків і 5 типів подій.
