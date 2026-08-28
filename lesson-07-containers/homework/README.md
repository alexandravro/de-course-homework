# Домашнє завдання — Заняття 07: Контейнери

## Що робимо

Контейнеризуємо готовий Python-скрипт і піднімаємо стек із двох сервісів у **Docker
Compose**: `ingestor` (ваш образ) завантажує годину подій **GitHub Archive** і вантажить
їх у сервіс `postgres`. Python-код **дано** — ви пишете лише інфраструктуру: `Dockerfile`,
`.dockerignore` і `docker-compose.yml`. Це рівно ті навички (порядок шарів і кешування,
non-root, healthcheck, networking між контейнерами, `depends_on`), на яких будується
наступне заняття — Airflow у Compose.

- **Специфікація і бали:** [`SPEC.md`](SPEC.md) — головний документ, читайте його.
- **Стартові файли (ваш код):** ця директорія (`homework/`)
- **Еталон (для самоперевірки після здачі):** [`../solution/`](../solution/)
- **Скрипт (дано, не редагувати):** [`app/gh_ingest.py`](app/gh_ingest.py)

## Передумови

Встановлений Docker з Docker Compose. Перевірте:

```bash
docker --version
docker compose version
```

## Як запустити

Ви працюєте у цій директорії (`homework/`):

```bash
cd lesson-07-containers/homework

# 1. Створіть .env із прикладу (docker compose читає його автоматично)
cp .env.example .env

# 2. Реалізуйте Dockerfile, .dockerignore і docker-compose.yml (див. SPEC.md)

# 3. Підніміть стек: build образу, старт Postgres, потім ingestor
docker compose up --build

# 4. Коли ingestor завершиться — перевірте дані в Postgres
docker compose exec postgres \
  psql -U gh -d gharchive \
  -c "SELECT event_type, count(*) FROM github_events GROUP BY 1 ORDER BY 2 DESC;"

# 5. Зупинити і прибрати (разом із volumes)
docker compose down -v
```

## Як підходити

1. **Спочатку Dockerfile.** Зберіть і перевірте окремо, без Compose:
   ```bash
   docker build -t gh-ingest:latest .
   ```
   Поки `docker build` не проходить — за Compose не беріться.
2. **Потім Postgres у Compose.** Підніміть лише БД і дочекайтесь `healthy`:
   ```bash
   docker compose up postgres
   docker compose ps      # STATUS має містити (healthy)
   ```
3. **Далі ingestor.** Додайте сервіс із `depends_on … service_healthy`. Якщо ingestor
   падає з помилкою підключення — майже завжди це `PGHOST` (має бути `postgres`, ім'я
   сервісу, а не `localhost`).
4. **Звіряйтеся з checkpoint** у `SPEC.md`: після успішного запуску — **211 466** рядків.
5. **Застрягли?** Підгляньте у [`../solution/`](../solution/) — там робочий еталон. Спочатку
   спробуйте самі.

## Самоперевірка

З кореня цієї директорії запустіть наскрізну перевірку — вона підніме ваш стек і перевірить
**результат у Postgres** (а не ваш Dockerfile):

```bash
./verify.sh
```

Зелений `PASS ✅` = стек робочий, дані на місці. Той самий скрипт є і в `../solution/` —
для звірки з еталоном.

## Що здавати

Pull request зі вмістом цієї директорії:

- `Dockerfile`, `.dockerignore`, `docker-compose.yml` (ваш код);
- **скриншоти** у `screenshots/` або в описі PR (див. `SPEC.md` → Завдання 5):
  1. фінал логів `docker compose up --build` із `Done. Loaded 211466 rows ...`;
  2. `docker compose ps` зі станом контейнерів;
  3. результат SQL-запиту з розбивкою по `event_type`.

Скрипт `app/gh_ingest.py` **не змінюйте** — він має лишитись таким, як виданий.

## Оцінювання — 100 балів (+10 бонус)

| # | Файл / крок | Що оцінюється | Балів |
|---|---|---|---|
| 1 | `Dockerfile` | базовий образ, порядок шарів, non-root, /cache, ENTRYPOINT | 30 |
| 2 | `.dockerignore` | виключення зайвого з build context | 10 |
| 3 | `docker-compose.yml` · `postgres` | образ, env, named volume, healthcheck | 20 |
| 4 | `docker-compose.yml` · `ingestor` | build, networking, `depends_on` healthy, volume | 25 |
| 5 | запуск + скриншоти | робочий стек, 211 466 рядків, розбивка по типах | 15 |
| | | **Разом** | **100** |
| — | Бонус | малий образ + робочий HEALTHCHECK | +10 |
