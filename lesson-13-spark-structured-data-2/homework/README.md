# Домашнє завдання — Заняття 13: Структуровані дані у Spark. Частина 2

## Що робимо

Будуємо **medallion pipeline** на Spark: **Bronze** (raw ingestion) → **Silver + Gold**
(dbt на Spark). Bronze **дано** — ви пишете **10 SQL-моделей** у [`dbt_lakehouse/`](dbt_lakehouse/)
плюс тести.

Головна відмінність від ДЗ занять 03 і 12: увесь корисний вміст події лежить у `payload`
як **сирий JSON-рядок**. Silver має його розібрати (`from_json`), розкласти масиви
(`explode`) і звести кілька типів подій до нормалізованих сутностей.

dbt працює через **`method: session`** — проти локальної SparkSession в одному процесі,
**без thrift-сервера**. Усі шари — Spark-таблиці у спільному `spark-warehouse/`.

- **Специфікація:** [`SPEC.md`](SPEC.md) — головний документ, читайте його.
- **Ваш код:** [`dbt_lakehouse/models/`](dbt_lakehouse/models/) — 10 заглушок, які треба замінити.
- **Bronze (дано, не чіпати):** [`bronze_job.py`](bronze_job.py),
  спільний помічник [`common/spark.py`](common/spark.py).

## Передумови

`uv` і Java (17/21). Ця директорія має власне `pyproject.toml`/`uv.lock`
(`pyspark`, `dbt-core`, `dbt-spark[session]`) — `uv run` сам піднімає ізольоване
середовище, окреме від решти курсу.

## Як запустити

**Усі команди — з кореня цієї директорії** (щоб усі кроки бачили один `spark-warehouse/`):

```bash
# 1. Bronze (дано): landing -> bronze.raw_events (payload зберігається сирим JSON-рядком)
uv run python bronze_job.py

# 2. Silver + Gold (ваш): один dbt build будує всі шари + тести
uv run dbt build --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse
```

## Як підходити

1. **Silver зверху вниз.** Спочатку `silver/events.sql` (базовий шар, `incremental`),
   потім `commits` → `pull_requests` → `issues` (усі три парсять `payload`).
   ```bash
   uv run dbt build --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse --select events
   ```
2. **Потім Gold по одній моделі.** Заглушки повертають 0 рядків — замінюйте їх запитами
   й звіряйтеся з checkpoint у `SPEC.md`.
3. **Тести** додайте у `silver/schema.yml`, `gold/schema.yml` і три singular-тести в
   `tests/` (див. `SPEC.md → «Тести»`).

### Пастки Spark SQL

- `` `distinct` `` — reserved word: у DDL-схемі `from_json` і в доступі до поля потрібні backticks.
- `from_json` мовчки повертає `NULL` для всієї структури, якщо JSON не збігається зі схемою —
  не `_corrupt_record`.
- `size(NULL) = -1` (не `NULL`) → `label_count` рахуйте через `CASE`.
- рядки для `union all` у `fact_repo_activity_daily` мають **точно** збігатися за порядком
  і типами колонок.
- `merge` як `incremental_strategy` на session-адаптері недоступний для parquet — беремо `append`.

## Самоперевірка

```bash
./verify.sh
```

`verify.sh` піднімає весь медальйон, перевіряє **інкрементальність** `silver.events`
(два прогони bronze — по одному файлу, потім решта), кількість рядків у всіх шарах,
ідемпотентність bronze і зелений `dbt build`. `PASS ✅` = ДЗ робоче.

## Що здавати

Pull request:
- `dbt_lakehouse/models/silver/*.sql` (4 моделі),
- `dbt_lakehouse/models/gold/*.sql` (6 моделей),
- `dbt_lakehouse/models/silver/schema.yml`, `dbt_lakehouse/models/gold/schema.yml`,
- `dbt_lakehouse/tests/*.sql` (3 singular-тести).

Файли `bronze_job.py`, `common/`, `dbt_project.yml`, `profiles.yml`, `sources.yml`,
макрос `generate_schema_name.sql` **не змінюйте**.
`spark-warehouse/`, `metastore_db/`, `derby.log` комітити не треба (в `.gitignore`).
