# Проєкт: medallion-pipeline для подій ride-hailing

## Що ви будуєте

Data platform для сервісу ride-hailing. Застосунки пасажира й водія та billing шлють події
про життєвий цикл поїздки: замовлено → прийнято → почато → завершено (або скасовано) → оплачено.
Ви приймаєте цей потік, складаєте його в **Bronze**, очищаєте в **Silver**, моделюєте
**star schema** в **Gold** і запускаєте все за розкладом.

Усе працює **локально, у Docker**. Жодних хмарних сервісів, акаунтів чи ключів.

```
 producer ──► Kafka ──► consumer ──► landing/ ──► Spark ──────► Postgres ──► dbt ──► Postgres
 (дано)    ride-events   (ваш)      NDJSON-файли  local mode   bronze.raw   Silver    silver.*
                                                  (ваш)        _events      + Gold    gold.*
                                                                                (ваші)
                          ▲                                        ▲              ▲
                          └─ крутиться в Docker сам ──┘            └── Airflow (ваш DAG) ──┘
```

| Шар | Технологія | Хто пише | Що всередині |
|---|---|---|---|
| Stream | Kafka, native Python | producer — **дано**, consumer — **ви** | події летять у топік `ride-events` |
| Landing | NDJSON-файли | consumer | сирі повідомлення «як є», файл на батч |
| Bronze | **Spark** (local mode) → Postgres | **ви** | `bronze.raw_events`: сирий JSON + метадані ingestion |
| Silver | **dbt** (Postgres) | **ви** | `silver.events`, `silver.rides`: очищено, типізовано, без дублікатів |
| Gold | **dbt** (Postgres) | **ви** | `gold.dim_*`, `gold.fact_ride`, `gold.agg_zone_hourly` |
| Orchestration | **Airflow** | **ви** | DAG: Bronze → Silver → Gold → звірка |

**Моделі dbt — incremental** (крім статичного довідника зон): DAG запускається кожні кілька хвилин
і кожного разу обробляє лише те, що з'явилося нового. Перебудова всього з нуля (`--full-refresh`) має давати **той самий
результат**, що й купа інкрементальних запусків, — це головна вимога проєкту.

## Три етапи

Проєкт розрахований на **два тижні** і складається з трьох частин, кожна — окремий Pull Request.
Складність кожної приблизно як одне звичайне ДЗ (другої — півтора).

| Етап | Що робите | Як перевіряється |
|---|---|---|
| **1. Ingest** | `stream/consumer.py` (Kafka → landing) і `bronze_job.py` (Spark → Bronze) | `./verify.sh ingest` |
| **2. Transform** | шість dbt-моделей (Silver і Gold) + два власні тести | `./verify.sh transform` |
| **3. Orchestrate** | `dags/rides_medallion.py` + `NOTES.md` | `./verify.sh orchestrate` |

Рекомендований графік: етап 1 — до кінця першого тижня, етап 2 — до середини другого, етап 3 —
до кінця. Етап 2 **не залежить** від етапу 1: для роботи над dbt є готові фікстури (див. нижче), тож
застряглий consumer вас не блокує. Етап 3 збирає все разом — `./verify.sh orchestrate` запускає ваші
`bronze_job.py` і dbt-моделі, тому потребує готових етапів 1 і 2.

Доки в етапі лишились заглушки, `./verify.sh <етап>` зупиняється одразу: Docker, Spark і dbt не
запускаються без рішення.

Повна специфікація — контракти, правила, контрольні числа — у **[SPEC.md](SPEC.md)**.

## Передумови

- **Docker** з Compose v2 (`docker compose version`): ≥ 8 GB RAM для контейнерів і ≈ 6 GB диска під
  образи (образ Airflow ≈ 3.6 GB)
- **uv** (менеджер Python-пакетів) і Python 3.12 — `uv sync` поставить залежності проєкту
- **Java 17** — для Spark (`java -version`)
- вільні порти: `5433` (Postgres), `9094` (Kafka), `8080` (Airflow)

## Структура

```
homework/                         # ← ТУТ ВИ ПРАЦЮЄТЕ
├── README.md, SPEC.md            # опис і специфікація
├── NOTES.md                      # ← ви заповнюєте: рішення, журнал використання ШІ
├── up.sh                         # дано: ідемпотентний запуск стеку (див. «Швидкий старт»)
├── verify.sh                     # перевірка етапів одним запуском
├── docker-compose.yml, docker/   # дано: Postgres, Kafka, producer, consumer, Airflow
├── common/                       # дано: конфігурація, локальна SparkSession
├── stream/
│   ├── events.py, producer.py    # дано: генератор подій і producer
│   └── consumer.py               # ← ЕТАП 1
├── bronze_job.py                 # ← ЕТАП 1
├── dbt_rides/
│   ├── models/silver/            # ← ЕТАП 2: events.sql, rides.sql
│   ├── models/gold/              # ← ЕТАП 2: dim_zone, dim_driver, fact_ride, agg_zone_hourly
│   ├── tests/                    # дано: 11 тестів; ви додаєте щонайменше 2 власні
│   └── (dbt_project, profiles, selectors, macros, seeds, sources, schema.yml — дано)
├── dags/rides_medallion.py       # ← ЕТАП 3
├── scripts/                      # дано: фікстури, скидання складу, перевірка DAG
├── data/fixtures/                # дано: 4 готові батчі landing-файлів
├── data/landing/                 # сюди пише consumer (у .gitignore)
└── tests/                        # дано: pytest-перевірки етапів
```

**Де писати код:** лише у файлах, позначених «ви». У моделях dbt і функціях Python стоїть заглушка
(`NotImplementedError` / `raise_compiler_error`) — замініть її реалізацією. Усе інше не змінюйте.

## Швидкий старт

```bash
uv sync                                   # залежності проєкту
./up.sh                                   # Postgres + Kafka + producer + consumer
docker compose logs -f consumer           # що зараз падає в landing
```

`./up.sh` **ідемпотентний**: його можна запускати скільки завгодно разів — він створить `.env` (uid/gid
для файлів landing) лише якщо його ще нема, нічого не перестворить без потреби, а `producer` не
опублікує корпус вдруге, якщо топік уже непорожній. `producer` публікує корпус із 5 745 подій
(~3 хвилини) і засинає; `consumer` крутиться безперервно.

| Команда | Що робить |
|---|---|
| `./up.sh` | стрімінг-стек: Postgres, Kafka, producer, consumer |
| `./up.sh --airflow` | плюс Airflow (UI: http://localhost:8080, `airflow` / `airflow`); чекає, поки DAG `rides_medallion` зареєструється, і вмикає його |
| `./up.sh --airflow --run` | плюс один запуск DAG одразу і очікування його завершення |
| `./up.sh --down` | зупинити все, **дані зберегти** |
| `./up.sh --reset` | знести все разом із даними (томи й `data/landing`) — чистий старт |

Airflow важчий, тож потрібен лише з етапу 3. DAG `rides_medallion` створюється **на паузі**:
`./up.sh --airflow` вмикає його сам; вручну — перемикачем в UI або
`docker compose exec airflow-scheduler airflow dags unpause rides_medallion` (лише коли DAG уже зʼявився
в UI: раніше команда мовчки нічого не робить). Запустити одразу, не чекаючи розкладу:
`docker compose exec airflow-scheduler airflow dags trigger rides_medallion`.

Опублікувати корпус ще раз свідомо (ті самі `event_id`: для Bronze це нові рядки, для Silver —
дублікати, які має прибрати дедуплікація):
`docker compose exec -e REPUBLISH=1 producer python -m stream.producer`.

## Як працювати

1. **Йдіть за етапами** і за `SPEC.md` зверху вниз. Кожен етап має свою команду `verify.sh`.
2. **Не чекайте на стрім.** Для етапів 2 і 3 є фікстури — чотири батчі landing-файлів, що імітують
   надходження даних у часі:
   ```bash
   uv run python -m scripts.reset_warehouse            # чистий склад
   uv run python -m scripts.make_fixtures              # (пере)генерує data/fixtures
   uv run python -m scripts.load_bronze_fixture 1 2    # завантажити батчі 1 і 2 в Bronze без Spark
   ```
   Так ви відтворюєте «дані приїжджають частинами» і бачите, як поводиться інкремент.
3. **dbt запускайте з обгортки** `./dbt.sh` — вона підставляє шляхи до проєкту й профілю:
   ```bash
   ./dbt.sh build --selector silver --indirect-selection cautious
   ./dbt.sh build --selector gold   --indirect-selection cautious
   ./dbt.sh test  --selector reconcile
   ```
4. **Числа зі `SPEC.md` — ваш компас.** Поки число не збіглося, трансформація ще не та.
5. **Дебаг:** `docker compose logs`, `psql` у контейнері
   (`docker exec -it fp-postgres psql -U taxi -d taxi_dwh`), `dbt compile` і `target/compiled/`.

## Використання ШІ

**ШІ-асистенти дозволені й заохочувані.** Ми оцінюємо ваше судження, а не швидкість друку:
уміння сформулювати задачу, перевірити результат і помітити, де згенерований код тільки
*здається* правильним.

Правила прості:

- **Відповідальність за код — ваша.** Здаєте ви, а не асистент.
- **Перевіряйте.** Тести й `verify.sh` написані так, щоб правдоподібний, але хибний код падав:
  дублікати, late events, часові пояси, повторні запуски, `kill -9` посеред роботи.
- **Ведіть журнал у `NOTES.md`**: як ви контролювали результати роботи ШІ та що робили з помилками (тест, запит, порівняння з числами).

## Якість коду і pre-commit

Код Python перевіряють **ruff** (lint і формат) та **mypy** (типи). Це частина `verify.sh`
(`./verify.sh lint`), а щоб не отримувати помилки в останню мить, підключіть хуки:

```bash
# з кореня git-репозиторію
uv run --project final-project/homework pre-commit install \
    --config final-project/homework/.pre-commit-config.yaml
```

Ручний прогін по всіх файлах: `./verify.sh lint`.

## Що здавати

Три Pull Request-и у **вашому** репозиторії, по одному на етап (гілки `project-1-ingest`,
`project-2-transform`, `project-3-orchestrate`), reviewer — `@desireoftheother`. У кожному:

- змінені файли зі списку «ви» для цього етапу;
- зелений `./verify.sh <етап>` (вставте фінальні рядки виводу в опис PR);
- для етапу 3 — заповнений `NOTES.md`.

Директорії `data/landing/`, `target/`, `logs/` комітити не треба — вони в `.gitignore`.

## Якщо щось не працює

- **`Cannot connect to the Docker daemon`** — запустіть Docker (Colima / Docker Engine).
- **Порт зайнятий** — змініть зовнішній порт у `docker-compose.yml` або зупиніть чужий сервіс.
- **Kafka не стартує** — Docker має мало пам'яті; збільште до 8 GB.
- **Spark: `JAVA_HOME is not set`** — поставте Java 17 і перевірте `java -version`.
- **dbt: `relation "bronze.raw_events" does not exist`** — Postgres піднявся не з цього
  `docker-compose.yml`; `docker compose down -v && docker compose up -d`.
- **Нічого не допомагає** — пишіть у чат у Slack, додайте вивід помилки.
