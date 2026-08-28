# SPEC.md — специфікація домашнього завдання (L12)

Головний документ ДЗ. Описує, що має повертати кожна функція у [`job.py`](job.py).

Ви реалізуєте десять функцій, позначених `raise NotImplementedError`. Оркестрація
(`build_spark`, `read_raw`, `main`) **дана** — вона складає з ваших функцій pipeline,
збирає marts у словник і передає його вашому writer-у.

Перевірок теж дві:

* `tests/test_functions.py` — unit-тести кожної функції на крихітних DataFrame
  у пам'яті. Запускати job для них не треба, вони працюють за секунди.
* `tests/test_outputs.py` — приймальні перевірки того, що job записав у `data/output/`.

---

## Дані (вже в репозиторії)

`data/landing/2024-01-15-{12,13,14}.json.gz` — детермінований зразок **GitHub Archive**
(по 12 000 рядків з кожної з трьох годин 15 січня 2024, UTC). Один рядок = одна подія
у форматі NDJSON. Сирі дані «брудні»: усі типи подій, дублікати, боти.

**Усього в landing: 36 000 рядків.** Усі контрольні числа нижче зафіксовані саме на
цьому зразку.

> Сесія працює в UTC (`spark.sql.session.timeZone` у `build_spark`). Без цього
> `date_trunc("hour", ...)` дав би різні значення на машинах у різних часових поясах.

---

## Pipeline

```
data/landing/*.json.gz                                       36 000
   │
   ├─ 1  event_schema()              явна схема читання
   │     read_raw(spark)             ДАНО
   ├─ 2  flatten(raw)                вкладені структури → пласкі колонки
   ├─ 3  clean(events)               фільтри якості + дедуплікація      29 750
   ├─ 4  with_derived(events)        repo_owner, is_bot, hour
   │
   ├─ 5  owner_totals(events)                                           14 822
   ├─ 6  top_repos_per_type(events, n)                                      25
   ├─ 7  enrich_top_repos(top, owners)
   ├─ 8  summary_slice(events, dimension)
   ├─ 9  build_summary(events, dimensions)                              25 102
   │
   └─ 10 write_outputs(marts)        запис усіх marts у data/output/
```

## Виходи job (пише `main`, читає `tests/test_outputs.py`)

Усі чотири пише ваш `write_outputs` (крок 10).

| Директорія в `data/output/` | Звідки | Рядків |
|---|---|---|
| `events/` (Parquet, `partitionBy` `event_type`) | кроки 2–4 | 29 750 |
| `owner_totals/` | крок 5 | 14 822 |
| `top_repos/` | кроки 6–7 | 25 |
| `summary/` | кроки 8–9 | 25 102 |

---

## Крок 1 — `event_schema()`

Поверніть **явну** `StructType` (schema-on-read, **без** `inferSchema`):

| Поле | Тип |
|---|---|
| `id` | `StringType` |
| `type` | `StringType` |
| `actor` | `StructType` з єдиним полем `login` (`StringType`) |
| `repo` | `StructType` з єдиним полем `name` (`StringType`) |
| `public` | `BooleanType` |
| `created_at` | `StringType` (парсинг — на кроці 2) |

Саме цю схему `read_raw` подає у `spark.read.schema(...).json(...)`. Решта полів
події (зокрема великий `payload`) не читається взагалі.

---

## Крок 2 — `flatten(raw)`

Розгорнути вкладені структури. Рівно шість колонок, **у цьому порядку**:

| Колонка | Джерело |
|---|---|
| `event_id` | `id` |
| `event_type` | `type` |
| `actor_login` | `actor.login` |
| `repo_name` | `repo.name` |
| `public` | `public` |
| `created_at` | `to_timestamp(created_at)` — тип `TimestampType` |

**Ніякої фільтрації.** На виході стільки ж рядків, скільки на вході (36 000).
Якщо `repo` у події відсутній — `repo_name` має стати `NULL`, а не зникнути.

---

## Крок 3 — `clean(events)`

Правила якості, застосовані до результату кроку 2:

1. лишити лише типи з `TARGET_EVENT_TYPES` (5 штук);
2. лишити лише події з `public = true`;
3. прибрати рядки, де `event_id`, `repo_name` або `created_at` — `NULL`;
4. дедуплікувати по `event_id`.

> **Пастка з трьохзначною логікою.** `public` може бути `NULL`. Умова
> `F.col("public") != False` таких рядків **не** залишить: порівняння з `NULL` дає
> `NULL`, а не `true`, і `filter` його відкидає. Рядки з `NULL` у `public` мають
> зникнути — але переконайтеся, що ви прибираєте їх свідомо, а не випадково.

**Checkpoint:** **29 750** рядків; рівно 5 типів; усі `event_id` унікальні.

---

## Крок 4 — `with_derived(events)`

Додати три колонки до результату кроку 3 (наявні колонки лишаються):

| Колонка | Правило |
|---|---|
| `repo_owner` | частина `repo_name` до `/` (`acme/api` → `acme`) |
| `is_bot` | `actor_login` закінчується на `BOT_SUFFIX` (`[bot]`) |
| `hour` | `created_at`, обрізаний до години — тип `TimestampType` |

`is_bot` має бути `false`, а не `NULL`, коли `actor_login` дорівнює `NULL`.

**Checkpoint:** ботських подій — **6 801**.

---

## Крок 5 — `owner_totals(events)`

Агрегат по власниках репозиторіїв. Грануляція: один рядок на `repo_owner`.

| Колонка | Правило |
|---|---|
| `repo_owner` | ключ групування |
| `owner_events` | `count(*)` |
| `owner_repos` | кількість унікальних `repo_name` |
| `owner_bot_events` | кількість подій, де `is_bot` = `true` |

**Checkpoint:** **14 822** рядки; `sum(owner_events) = 29 750`.

---

## Крок 6 — `top_repos_per_type(events, n)`

Топ-`n` репозиторіїв за кількістю подій **у межах кожного** `event_type` — через
**window function**. Колонки: `event_type`, `repo_name`, `repo_event_count`, `rank`.

Патерн: агрегація по `(event_type, repo_name)`, потім
`row_number() OVER (PARTITION BY event_type ORDER BY repo_event_count DESC, repo_name ASC)`,
фільтр `rank <= n`.

Тай-брейк за `repo_name` за зростанням — обов'язковий: без нього результат
недетермінований і тест на рангах буде то зелений, то червоний.

**Checkpoint:** при `n = 5` — **25** рядків (5 типів × 5), ранги `1..5` у кожному типі.

---

## Крок 7 — `enrich_top_repos(top_repos, owners)`

Приєднати до топу підсумки власника. `owners` — маленька таблиця, тож join має бути
**broadcast**.

1. дістати `repo_owner` з `repo_name` (те саме правило, що на кроці 4);
2. **LEFT JOIN** з `owners` по `repo_owner`;
3. зібрати результат.

| Колонка | Правило |
|---|---|
| `event_type`, `repo_name`, `repo_owner`, `repo_event_count`, `rank` | з топу |
| `owner_events` | з `owners`, за відсутності збігу → `0` |
| `owner_repos` | з `owners`, за відсутності збігу → `0` |
| `owner_share` | `round(repo_event_count / owner_events, 4)`; якщо `owner_events` = 0 → `NULL` |

LEFT JOIN без збігу дає `NULL` — і ділення на нього (чи на `0`) не має валити job.
На справжніх даних збігаються всі рядки, але unit-тест перевіряє саме випадок без збігу.

**Checkpoint:** **25** рядків; `owner_share` у межах `(0, 1]`, без `NULL`.

---

## Крок 8 — `summary_slice(events, dimension)`

Один зріз підсумкової таблиці за виміром, **назва якого приходить аргументом**.
Та сама функція має працювати для будь-якого імені з `SUMMARY_DIMENSIONS`.

| Колонка | Правило |
|---|---|
| `dimension` | літерал — назва виміру (`"event_type"`, `"repo_owner"`, …) |
| `dimension_value` | значення цього виміру, приведене до `StringType` |
| `events` | `count(*)` |
| `distinct_repos` | кількість унікальних `repo_name` |

Приведення до рядка потрібне, бо виміри мають різні типи: `hour` — це `TimestampType`,
решта — `StringType`, а в кроці 9 вони опиняться в одній колонці.

---

## Крок 9 — `build_summary(events, dimensions)`

Усі зрізи, зібрані в **одну** таблицю. Колонки — ті самі чотири, що на кроці 8.
Грануляція: один рядок на пару `(dimension, dimension_value)`.

**Реалізація на ваш вибір.** Найпростіший шлях — пройтися циклом по `dimensions`,
зібрати список DataFrame і склеїти їх (`unionByName`). Це повністю коректне рішення.
Є й інші — див. бонус.

**Checkpoint:** **25 102** рядки. Розподіл по вимірах (для самоперевірки):

| `dimension` | рядків | `sum(events)` |
|---|---|---|
| `event_type` | 5 | 29 750 |
| `hour` | 3 | 29 750 |
| `actor_login` | 10 272 | 29 750 |
| `repo_owner` | 14 822 | 29 750 |

Кожен вимір ріже той самий набір подій — тому сума завжди одна й та сама. Якщо
десь не 29 750, шукайте зайвий чи відсутній фільтр саме в цьому зрізі.

---

## Крок 10 — `write_outputs(outputs)`

Запис усіх marts. На вхід — словник `{ім'я: (DataFrame, колонка партиціонування | None)}`;
`main` формує його сам і більше нічого про запис не знає.

Для кожного запису:

* директорія — `data/output/<ім'я>/`, формат Parquet, режим `overwrite`;
* якщо колонка партиціонування `None` — писати **одним** файлом (`coalesce(1)`);
* якщо колонка задана — `partitionBy` за нею, і перед тим `repartition` за тією самою
  колонкою.

> `repartition` перед `partitionBy` — не косметика. Без нього кожен task пише власний
> файл у кожну партицію: 4 task-и × 5 типів = 20 дрібних файлів замість 5.

**Checkpoint:** `events/` — 5 директорій `event_type=…`; `owner_totals/`, `top_repos/`
і `summary/` — рівно по одному `.parquet` у кожній.

---

## Бонус (необов'язково)

Рішення з `unionByName` сканує `events` по разу на кожен вимір. Той самий результат
можна отримати **за один прохід**. Два робочі підходи:

* згенерувати масив пар `(dimension, dimension_value)` і розкрити його через
  `explode` — далі один `groupBy`;
* `groupingSets` (DataFrame API, Spark 4.0+).

Якщо беретеся: додайте другу реалізацію окремою функцією і покладіть поряд
`ANSWERS.md` з `explain()` обох варіантів, кількістю `Exchange` у планах і часом
виконання. Цінний тут саме розбір різниці планів, а не сам факт прискорення.

---

**Definition of done:** `./verify.sh` (з кореня `homework/`) зелений — unit-тести
проходять, job відпрацював, усі артефакти на місці й контрольні числа збіглися.
