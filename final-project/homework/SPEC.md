# SPEC.md — специфікація проєкту

[README.md](README.md) відповідає на «що і як запустити», цей файл —
на «що саме має вийти»: формати даних, контракти шарів, правила, контрольні числа.

Усі команди — з кореня `homework/`.

---

## 1. Дані

### 1.1 Конверт події

Кожне повідомлення в Kafka — один JSON-об'єкт:

| Поле | Тип | Опис |
|---|---|---|
| `event_id` | string | унікальний ідентифікатор події; у дубліката **той самий** |
| `event_type` | string | один із шести типів (нижче) |
| `ride_id` | string | поїздка, до якої належить подія |
| `source` | string | хто надіслав: `rider_app`, `driver_app`, `billing`, `loadtest` |
| `occurred_at` | string | коли подія **сталася** (ISO-8601, UTC, `…Z`) — event time |
| `payload` | object | вміст залежить від `event_type` — **напівструктурований** |

### 1.2 Типи подій і `payload`

| `event_type` | Хто шле | Що в `payload` (JSON-шляхи) |
|---|---|---|
| `ride_requested` | rider_app | `rider.id`, `rider.platform`, `rider.app_version`, `pickup.zone_id`, `dropoff.zone_id`, `requested_vehicle`, `surge_estimate` |
| `ride_accepted` | driver_app | `driver.id`, `driver.rating`, `driver.vehicle.type`, `driver.vehicle.medallion`, `eta_seconds` |
| `ride_started` | driver_app | `driver.id`, `pickup.zone_id` (**фактична** посадка), `odometer_km` |
| `ride_completed` | driver_app | `driver.id`, `dropoff.zone_id` (**фактична** висадка), `distance_km`, `fare.{amount, surge_multiplier, tolls, tip, total, currency}` |
| `ride_cancelled` | rider_app / driver_app | `cancelled_by`, `reason`, `stage` |
| `payment_captured` | billing | `payment.{method, amount, currency, psp_reference}` |

Зони (`zone_id`) — це `location_id` із довідника таксі-зон Нью-Йорка (`dbt_rides/seeds/`), 1–265.

### 1.3 Що не так із потоком

Джерело — «чужа» система: вона шле те, що шле, і виправити це там ви не можете. Бруд навмисний:

| Проблема | Як виглядає | Що з цим робить pipeline |
|---|---|---|
| **Дублікати** | ~3% подій публікуються двічі з тим самим `event_id` | Silver лишає один рядок на `event_id` |
| **Late events** | ~4% подій приходять на 5–40 хвилин пізніше за свої «сусідні» події | інкремент за **часом ingestion**, а не за `occurred_at` |
| **Порядок** | `ride_completed` може прийти раніше за `ride_started` | згортка поїздки не залежить від порядку прибуття |
| **Тестовий трафік** | усі події ~2% поїздок мають `source = "loadtest"` (rider `test-N`) | Silver відкидає їх **цілком**, разом із billing-подіями |
| **NULL-чайові** | готівкові поїздки: `fare.tip = null` (не 0) | лишається NULL; у сумах — як 0 |
| **Факт ≠ намір** | у ~5% поїздок зона з `ride_started` / `ride_completed` інша, ніж із `ride_requested` | береться **фактична** |
| **Оцінка ≠ факт** | `surge_estimate` (замовлення) ≠ `fare.surge_multiplier` (завершення) | у розрахунках — фактичний множник |

### 1.4 Часова модель

- `occurred_at` (event time) прив'язаний до **фіксованого** якоря `2024-01-15T08:00:00Z` і не
  залежить від того, коли ви запустили стек. Тому контрольні числа нижче однакові в усіх.
- Час **публікації** стиснуто в `SPEED`=60 разів: корпус (~2.8 год подій) їде в Kafka ~3 хвилини.
- `dt=` і `hour=` у шляху landing-файлу — це час **запису** файлу, а не час події. Подія, що
  сталася о 08:12, але приїхала пізно, лежить у файлі з іншим `hour=`. Тому шари нижче
  партиціонують і фільтрують за власними полями, а не за шляхом.

---

## 2. Етап 1а — Kafka → landing (`stream/consumer.py`)

### 2.1 Топік

`ride-events`, 3 партиції, ключ повідомлення — `ride_id` (усі події поїздки в одній партиції),
значення — однорядковий JSON. Топік створює `kafka-init` у `docker-compose.yml`.

### 2.2 Контракт landing-зони

```
data/landing/dt=YYYY-MM-DD/hour=HH/part-p{partition}-o{first:012d}-o{last:012d}.ndjson
```

- `dt`, `hour` — UTC-час **запису** файлу.
- **Один файл на партицію Kafka на батч.** `first` і `last` — перший і останній офсети файлу.
  Ім'я — це діапазон офсетів: лог Kafka незмінний, тож те саме ім'я = той самий вміст.
  *Чому не лише `first`:* файл, переписаний після збою довшим, отримав би те саме ім'я; Bronze
  ідемпотентний за іменем і пропустив би його — хвіст батча тихо загубився б.
- **NDJSON:** один JSON-об'єкт на рядок, рядок завершується рівно одним `\n`. Записи всередині файлу — у
  порядку офсетів.
- **Атомарність:** пишете в `*.ndjson.tmp`, `fsync`, потім `os.replace`. Spark ніколи не має
  бачити напівзаписаний файл. Збій посеред запису не лишає ні готового файлу, ні `.tmp`.
- **At-least-once:** офсети комітяться **після** того, як файл записано (синхронно). Падіння в
  будь-який момент, зокрема `kill -9`, може дати дублікати, але **не втрати**.
- Батч закривається за `BATCH_MAX_MESSAGES` (500) або `BATCH_MAX_SECONDS` (10).

### 2.3 Що реалізувати

| # | Що | Вимоги |
|---|---|---|
| 1 | `landing_path(base, ingested_at, partition, first_offset, last_offset)` | шлях за контрактом |
| 2 | `write_batch(base, ingested_at, records)` | `records` — `(partition, offset, value_bytes)`; групує за партицією, сортує за офсетом, пише атомарно, повертає створені файли; повторний виклик із тим самим батчем нічого не дублює |
| 3 | `flush()` у `main()` | записати → **лише потім** закомітити офсети |

Цикл читання, сигнали й вихід по idle (`IDLE_EXIT_SECONDS`) дано.

---

## 3. Етап 1б — Spark → Bronze (`bronze_job.py`)

### 3.1 Таблиця Bronze (дано)

Таблицю створює платформа при першому старті Postgres (`docker/postgres-init.sql`), Spark лише
дописує в неї:

```sql
bronze.raw_events (
  event_id      text        NOT NULL,
  event_type    text        NOT NULL,
  ride_id       text,
  occurred_at   timestamptz,
  source        text,
  payload       text,           -- СИРИЙ JSON-рядок
  _source_file  text        NOT NULL,   -- шлях відносно landing/, напр. dt=…/hour=…/part-….ndjson
  _ingested_at  timestamptz NOT NULL    -- один для всіх рядків одного запуску
)
```

### 3.2 Контракт шару

- **Читання з явною схемою.** Ніякого `inferSchema`: схема — це контракт, а не здогадка.
- **`payload` зберігається сирим JSON-рядком** і не парситься. Вкладений об'єкт кладіть у колонку
  типу `StringType` — Spark віддасть його текст як є. Bronze нічого не виправляє й не фільтрує:
  дублікати й тестовий трафік тут **лишаються**.
- Метадані ingestion: `_source_file` (відносний шлях — щоб ключ збігався на ноутбуці й у
  контейнері) і `_ingested_at` (`current_timestamp()`).
- **Читайте лише `*.ndjson`.** Файли `*.ndjson.tmp` — це in-flight записи consumer-а.
- **Ідемпотентність за файлом:** файл, що вже завантажено (`_source_file` є в Bronze), вдруге не
  потрапляє. Повторний запуск без нових файлів нічого не змінює.
- **Атомарний append:** або всі нові рядки запису потрапили в таблицю, або жодного. Збій
  посеред запису не лишає в Bronze напівзавантажений батч, а повторний запуск після збою
  донавантажує рівно те, чого бракує — без дублікатів і без втрат.
- Spark — у **local mode** (`common/spark.py`, дано), запис у Postgres — через JDBC.

### 3.3 Що реалізувати

| # | Що | Вимоги |
|---|---|---|
| 4 | `EVENT_SCHEMA` і `read_landing(spark, landing_dir)` | явна схема, `payload` — рядок, `occurred_at` → timestamp, `_source_file`, `_ingested_at` |
| 5 | `select_new(df, already_loaded)` | лишає лише рядки з файлів, яких ще нема в Bronze |
| 6 | `write_bronze(df)` | append у `bronze.raw_events` **однією транзакцією** |

`loaded_files()` і `main()` дано.

---

## 4. Етап 2 — dbt: Silver і Gold

Bronze для dbt — це `source('bronze', 'raw_events')`: його пише Spark, dbt лише читає.

### 4.1 Загальні правила інкременту

Усі моделі, що ростуть разом зі стрімом (`silver.events`, `silver.rides`, `gold.dim_driver`,
`gold.fact_ride`, `gold.agg_zone_hourly`), — `incremental`, `incremental_strategy='delete+insert'`,
з `unique_key`. Єдиний виняток — `gold.dim_zone`: це статичний довідник із seed, він `table`.

1. **Межа інкременту — `_ingested_at`, а не `occurred_at`.** Late event має старий `occurred_at`,
   але приїжджає в новому батчі; фільтр за event time його загубив би. Кожна incremental-модель бере
   лише рядки, у яких `_ingested_at` вищий за максимум, що вже є в ній самій (`high_watermark()` у
   `macros/`). Перший запуск (таблиці ще немає) будує все.
2. **Нова подія змінює цілу сутність.** Запізніле `payment_captured` дописує `paid_at` до
   поїздки, яку ви збудували раніше. Перебудовуйте сутності, яких торкнулися нові дані, з **усієї**
   їхньої історії, а не з нового батча.
3. **`--full-refresh` дає той самий результат**, що й будь-яка послідовність інкрементальних
   запусків (порівнюються бізнес-колонки; `_ingested_at`, `_loaded_at`, `_source_file` — ні).
4. **Повторний запуск без нових даних нічого не змінює.** Ретрай в Airflow безпечний.
5. **Час — у UTC.** Дата з `timestamptz` без явного часового поясу залежить від налаштувань
   сесії клієнта; використовуйте макрос `utc_date()`.
6. `_loaded_at` = `'{{ run_started_at }}'::timestamptz` (один момент на весь запуск).

### 4.2 `silver.events`

**Grain:** одна подія (`event_id`). `unique_key = event_id`.

| Колонка | Тип | Джерело |
|---|---|---|
| `event_id`, `event_type`, `ride_id`, `source` | text | Bronze |
| `occurred_at` | timestamptz | Bronze |
| `occurred_date` | date | `occurred_at` за UTC |
| `payload` | **jsonb** | Bronze `payload` (text → jsonb) |
| `_source_file`, `_ingested_at` | text, timestamptz | Bronze |
| `_loaded_at` | timestamptz | запуск dbt |

Правила: відкинути `source = 'loadtest'`, рядки без `occurred_at` або без `ride_id`; лишити один
рядок на `event_id`. У межах одного запуску — найраніший `_ingested_at` (за рівності —
`_source_file`); дублікат, що приїхав у пізнішому запуску, або замінює старий рядок, або
пропускається: вміст події незмінний, обидва варіанти прийнятні.

### 4.3 `silver.rides`

**Grain:** одна поїздка (`ride_id`). `unique_key = ride_id`. Життєвий цикл із шести типів подій
згортається в один рядок.

| Колонка | Тип | Правило |
|---|---|---|
| `ride_id` | text | |
| `rider_id`, `rider_platform`, `app_version`, `requested_vehicle` | text | з `ride_requested` |
| `driver_id` | text | з `ride_accepted` / `ride_started` / `ride_completed` |
| `status` | text | `completed` → `cancelled` → `in_progress` → `accepted` → `requested` (перше, що виконано: є `ride_completed`; є `ride_cancelled`; є `ride_started`; є `ride_accepted`; інакше `requested`) |
| `requested_at`, `accepted_at`, `started_at`, `completed_at`, `cancelled_at`, `paid_at` | timestamptz | `occurred_at` відповідної події |
| `pickup_zone_id`, `dropoff_zone_id` | int | **фактична** зона (`ride_started` / `ride_completed`), а якщо її ще нема — заявлена (`ride_requested`) |
| `wait_seconds` | int | `accepted_at − requested_at` |
| `trip_seconds` | int | `completed_at − started_at` |
| `surge_estimate` | numeric | з `ride_requested` |
| `distance_km` | numeric(8,2), `surge_multiplier` numeric(4,2) | з `ride_completed` |
| `fare_amount`, `tolls_amount`, `tip_amount`, `total_amount` | numeric(10,2) | з `ride_completed.fare`; `tip_amount` лишається NULL для готівки |
| `currency` | text | `fare.currency` |
| `cancelled_by`, `cancel_reason`, `cancel_stage` | text | з `ride_cancelled` |
| `payment_method`, `psp_reference` | text | з `payment_captured` |
| `payment_amount` | numeric(10,2) | `payment.amount` |
| `_ingested_at` | timestamptz | **максимум** `_ingested_at` усіх подій поїздки |
| `_loaded_at` | timestamptz | запуск dbt |

Правила: поїздка з'являється, **коли прийшла її `ride_requested`** (події без запиту чекають у
`silver.events`); порядок прибуття подій не має значення.

### 4.4 Gold

| Модель | Матеріалізація | Grain |
|---|---|---|
| `gold.dim_zone` | `table` | зона (плюс член `-1` «Unknown») |
| `gold.dim_driver` | `incremental`, `unique_key = driver_key` | водій (плюс член `'unknown'`); поточні атрибути |
| `gold.fact_ride` | `incremental`, `unique_key = ride_id` | поїздка (accumulating snapshot) |
| `gold.agg_zone_hourly` | `incremental` | (`requested_hour`, `pickup_zone_key`) |

**`dim_zone`:** `zone_key` (int, = `location_id`), `borough`, `zone_name`, `service_zone`; порожні
значення → `'Unknown'`; додатковий рядок `-1`.

**`dim_driver`:** `driver_key`, `vehicle_type`, `medallion`, `latest_rating`, `first_seen_at`,
`last_seen_at`, `_ingested_at`, `_loaded_at` — за подіями `ride_accepted`; «останній» рейтинг
визначається за `occurred_at`, а не за порядком прибуття. Тому інкремент має перераховувати водія
з **усієї** його історії, а не з нового батча: інакше запізніла подія зі старим часом затре новіше
значення. Додатковий рядок `'unknown'` — для поїздок, скасованих до прийняття; він має бути в
таблиці рівно один раз після будь-якої кількості запусків.

**`fact_ride`:** колонки `silver.rides` + ключі до вимірів (без зайвих
колонок): `ride_id`, `pickup_zone_key`, `dropoff_zone_key`, `driver_key`, `rider_id`, `status`, шість
віх (`requested_at` … `paid_at`), `requested_date` (UTC), `requested_hour` (`timestamptz`, початок години за
UTC), `wait_seconds`, `trip_seconds`, `distance_km`, `fare_amount`, `surge_multiplier`,
`tolls_amount`, `tip_amount`, `total_amount`, `currency`, `payment_method`, `payment_amount`,
`cancelled_by`, `cancel_reason`, `cancel_stage`, `_ingested_at`, `_loaded_at`. Ключ, якого нема
у виміру, складається в член `-1` / `'unknown'` (а не губить рядок).

**`agg_zone_hourly`:** `requested_hour`, `pickup_zone_key`, `rides_requested`, `rides_completed`,
`rides_cancelled`, `gross_revenue` (сума `total_amount` завершених), `tips` (сума `tip_amount`
завершених), `avg_wait_seconds`, `_ingested_at`, `_loaded_at`. Вітрина має збігатися з
перерахунком із `fact_ride` **рядок у рядок після будь-якого інкременту**.

> Подумайте: що трапляється з вітриною, коли поїздка **змінює зону посадки** (`ride_started`
> приїхав пізніше за `ride_requested`)? Які рядки вітрини треба перерахувати?

### 4.5 Тести dbt

`schema.yml` (generic-тести) і `dbt_rides/tests/` (11 singular-тестів) дано — це виконувана
специфікація. Міжшарові звірки мають тег `reconcile` і йдуть окремим кроком після Gold.

**Ваше завдання:** додати **щонайменше два власні singular-тести** в `dbt_rides/tests/` — про
правило, яке ви вважаєте важливим і яке дані тести не покривають. Тест має проходити на правильних
даних і **падати на зламаних** (перевірте, навмисно зламавши модель). Опишіть у `NOTES.md`, що
кожен ловить.

> **Пастка `cautious`.** Тест, що посилається на моделі з різних шарів (скажімо, `events` із
> Silver і `dim_driver` із Gold), не запуститься ні кроком `silver`, ні кроком `gold`: під
> `--indirect-selection cautious` тест біжить лише тоді, коли в прогоні є **всі** його parent-и.
> Він мовчки нічого не перевірятиме. Повісьте на такий тест тег `reconcile`:
> `{{ config(tags=['reconcile']) }}`. `./verify.sh transform` перевіряє, що кожен тест у
> `dbt_rides/tests/` виконується якимось кроком DAG-у.

---

## 5. Етап 3 — Airflow (`dags/rides_medallion.py`)

| Параметр | Значення |
|---|---|
| `dag_id` | `rides_medallion` |
| Розклад | кожні 5 хвилин |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `default_args` | `retries ≥ 1`, `retry_delay`, `execution_timeout` |

Задачі й порядок: **`bronze_spark` → `bronze_contract` → `silver` → `gold` → `reconcile`**.

| Задача | Що робить |
|---|---|
| `bronze_spark` | `python bronze_job.py` (Spark, local mode, всередині контейнера Airflow) |
| `bronze_contract` | `dbt test --select source:bronze` — контракт Bronze |
| `silver` | `dbt build --selector silver` |
| `gold` | `dbt build --selector gold` |
| `reconcile` | `dbt test --selector reconcile` — звірка між шарами |

Вимоги:

- dbt викликається за **селекторами** з `selectors.yml`, а не переліком моделей у DAG-у.
- dbt-задачі з `build` використовують `--indirect-selection cautious`. Поясніть у `NOTES.md`,
  чому (підказка: тест із parent-ом в іншому шарі).
- Стан «що вже оброблено» живе в даних, а не в розкладі: **повторний запуск без нових файлів
  нічого не змінює**, а ретрай будь-якої задачі безпечний.
- DAG викликається без `data_interval_start/end`. У `NOTES.md` поясніть, чому для цього джерела
  вікно за інтервалом розкладу не підходить.

Контейнер Airflow має Java, `pyspark`, JDBC-драйвер Postgres і dbt (в окремому venv) — див.
`docker/Dockerfile.airflow`. Проєкт змонтовано в `/opt/airflow/project`.

---

## 6. Контрольні числа

### 6.1 Корпус

| | |
|---|---|
| повідомлень опубліковано (= рядків у Bronze після чистого прогону) | **5 745** |
| унікальних `event_id` | **5 587** (158 дублікатів) |
| рядків із `source = 'loadtest'` (з дублікатами) | **145** |

### 6.2 Фінальний стан (усі дані)

| Таблиця | Рядків | Додатково |
|---|---:|---|
| `bronze.raw_events` | 5 745 | |
| `silver.events` | 5 447 | |
| `silver.rides` | 1 171 | `completed` 1 008 · `cancelled` 163 |
| `gold.dim_zone` | 266 | 265 зон + член `-1` |
| `gold.dim_driver` | 81 | 80 водіїв + член `'unknown'` |
| `gold.fact_ride` | 1 171 | |
| `gold.agg_zone_hourly` | 351 | 2 години × зони |

Суми й перевірні зрізи (завершені поїздки):

| | |
|---|---|
| `sum(total_amount)` | **48 972.86** |
| `sum(tip_amount)` | **4 585.09** |
| завершених із `tip_amount IS NULL` (готівка) | **217** |
| поїздок з `paid_at IS NOT NULL` | **1 008** |
| поїздок, де фактична зона посадки ≠ заявленої | **62** (висадки: **39**) |
| топ-3 зони посадки за виручкою | `237` — 2 522.77 · `161` — 2 505.16 · `186` — 2 465.43 |
| `sum(rides_requested)` у вітрині | 1 171 · `sum(gross_revenue)` = 48 972.86 |

### 6.3 Фікстури: стан після кожного батча

Фікстури (`data/fixtures/batch_1 … batch_4`) — це корпус, розрізаний **за порядком публікації** на
чотири частини. Запізнілі події й дублікати опиняються в пізніших батчах, ніж решта подій їхньої
поїздки. Кумулятивно після батча *N*:

| Після батча | рядків Bronze | `silver.events` | `silver.rides` | `completed` | `cancelled` | `paid_at` заповнено |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 437 | 1 367 | 393 | 137 | 57 | 129 |
| 2 | 2 874 | 2 717 | 695 | 371 | 92 | 359 |
| 3 | 4 311 | 4 071 | 987 | 624 | 131 | 615 |
| 4 | 5 745 | 5 447 | 1 171 | 1 008 | 163 | 1 008 |

Проміжні статуси (`in_progress`, `accepted`, `requested`) існують лише поки поїздка не завершилась;
після батча 4 їх нема.

---

## 7. Як це перевіряється

`./verify.sh <етап>` — повна перевірка одним запуском. Зелений `verify.sh` = етап зараховано.

Якщо в етапі ще лишились заглушки, `verify.sh` зупиняється **одразу**: Docker, Spark і dbt не
запускаються, доки рішення нема. Етап 3 збирає все разом (ваші `bronze_job.py` і dbt-моделі), тому
потребує готових етапів 1 і 2.

| Команда | Що робить |
|---|---|
| `./verify.sh lint` | `ruff check`, `ruff format --check`, `mypy` |
| `./verify.sh ingest` | unit-тести consumer-а; Kafka e2e (усі повідомлення в landing; **збій запису не «з'їдає» повідомлення** — офсети комітяться лише після запису; **`kill -9` не губить нічого**); Spark-job на фікстурах: явна схема, `payload` без втрат, ідемпотентний повторний запуск, **збій посеред запису не лишає напівзавантаженого батча**, `*.tmp` ігноруються |
| `./verify.sh transform` | кожен dbt-тест виконується якимось кроком DAG-у → склад із нуля → фікстури батч за батчем із `dbt build` після кожного → після кожного батча стан збігається з таблицею 6.3 і всі dbt-тести зелені → фінальні числа 6.2 → **повторний запуск без змін нічого не змінює** → **`--full-refresh` дає ті самі дані, що й інкремент**. dbt запускається з нестандартним часовим поясом сесії (`PGTZ=Pacific/Chatham`, UTC+13:45): дати й межі годин мають залежати лише від UTC |
| `./verify.sh orchestrate` | структура DAG-у; запуски `airflow dags test` поспіль на фікстурах: після перших двох батчів — стан таблиці 6.3, після всіх — фінальні числа, без нових файлів — жодних змін. Перевірка сама ставить DAG на паузу, щоб scheduler не запускав його паралельно |
| `./verify.sh` | усе разом |

Що навмисно перевіряється, бо саме тут правдоподібний код виявляється хибним: порядок «запис →
commit» у consumer-і, ім'я landing-файлу, збій посеред запису (і в consumer-і, і в Spark),
`kill -9`, `payload` після проходу через Spark, повторний запуск, фільтр інкременту за
`_ingested_at` (а не `occurred_at`), згортка поїздки з усієї її історії, перерахунок цілих годин
вітрини, «останній» рейтинг за `occurred_at`, `NULL` замість `0` у чайових, фактичний (а не
оцінений) множник, дата й година за UTC при нестандартному часовому поясі сесії, тест, який
жоден крок DAG-у ніколи не запускає.
