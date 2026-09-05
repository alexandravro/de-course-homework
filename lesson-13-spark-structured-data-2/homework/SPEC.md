# SPEC.md — специфікація домашнього завдання (L13)

Головний документ ДЗ. Ви будуєте **medallion pipeline** на Spark: Bronze → Silver → Gold,
де увесь transform-шар — **один dbt-проєкт, що запускається проти SparkSession**
(`method: session`, без thrift-сервера).

**Bronze дано** (`bronze_job.py`). Ви пишете **10 моделей** Silver і Gold у `dbt_lakehouse/`
плюс тести. Головна відмінність від ДЗ занять 03 і 12: тут дані **напівструктуровані** —
увесь корисний вміст події лежить у `payload` як **сирий JSON-рядок**, і Silver має його
розібрати (`from_json`), розкласти масиви (`explode`) і звести кілька типів подій до
нормалізованих сутностей.

Усі кроки працюють зі спільним `spark-warehouse/` поточної директорії.
**Запускайте все з кореня `homework/`.**

---

## Дані та шари

`data/landing/` — зразок **GitHub Archive** за три години 15 січня 2024 (12:00–14:59 UTC),
36 000 подій NDJSON. Дані «брудні»: усі 15 типів подій, дублікати, боти, приватні події.

| Шар | Таблиця | Хто робить |
|---|---|---|
| Bronze | `bronze.raw_events` | **дано** (`bronze_job.py`): читає landing з явною схемою, `payload` зберігає як **сирий JSON-рядок** (нічого не парсить), додає `_ingested_at` / `_source_file`, ідемпотентний append |
| Silver | `silver.events`, `silver.commits`, `silver.pull_requests`, `silver.issues` | **ви** |
| Gold | `gold.dim_repo`, `gold.dim_actor`, `gold.dim_date`, `gold.fact_commit`, `gold.fact_pull_request`, `gold.fact_repo_activity_daily` | **ви** |

**Контракт Bronze** (нагадування з knowledge.md): Bronze **нічого не виправляє** і не парсить
`payload`. Уся робота з вкладеними структурами — у Silver.

**Checkpoint шарів:** `bronze.raw_events` = **36 000**.

### Схеми для `from_json` (дано — не виводьте самі)

Spark SQL `from_json(payload, '<ddl>')` приймає DDL-рядок. Використовуйте рівно ці три
(зайві поля payload не читаємо):

```
-- PUSH_SCHEMA (payload події PushEvent)
-- `distinct` — reserved word Spark SQL, тому в backticks і у схемі, і при доступі до поля
struct<
  size:int, distinct_size:int, ref:string,
  commits:array<struct<
    sha:string, message:string, `distinct`:boolean,
    author:struct<name:string, email:string>
  >>
>

-- PR_SCHEMA (payload події PullRequestEvent)
struct<
  action:string, number:int,
  pull_request:struct<
    state:string, title:string, draft:boolean, merged:boolean,
    created_at:string, closed_at:string, merged_at:string,
    additions:int, deletions:int, changed_files:int,
    commits:int, comments:int, review_comments:int,
    author_association:string,
    user:struct<login:string>,
    labels:array<struct<name:string>>
  >
>

-- ISSUE_SCHEMA (payload подій IssuesEvent та IssueCommentEvent)
struct<
  action:string,
  issue:struct<
    number:int, title:string, state:string,
    created_at:string, closed_at:string, comments:int,
    author_association:string,
    user:struct<login:string>,
    labels:array<struct<name:string>>
  >
>
```

---

## Крок 1 — `silver.events` · базовий шар подій

`{{ source('bronze', 'raw_events') }}` → чистий плаский шар **без парсингу payload**
(payload переноситься далі як є, для кроків 2–4).

**Матеріалізація:** `incremental`, `incremental_strategy='append'`.
У `is_incremental()`-гілці брати лише нові рядки:
`where _ingested_at > (select max(_ingested_at) from {{ this }})`.

> `merge` на session-адаптері доступний лише для delta/iceberg/hudi; на parquet —
> `append`. Крос-батчевих дублів немає, бо bronze ідемпотентний за `_source_file`,
> а `is_incremental()`-фільтр відрізає вже завантажені події.

**Колонки:** `event_id` (`id`), `event_type` (`type`), `actor_login` (`actor.login`),
`repo_name` (`repo.name`), `repo_owner` (частина `repo_name` до `/`), `created_at`
(`to_timestamp`), `payload` (рядок, як є), `_ingested_at`, `_source_file`.

**DQ-фільтри:**
- лишити лише 6 типів: `PushEvent`, `PullRequestEvent`, `IssuesEvent`,
  `IssueCommentEvent`, `WatchEvent`, `ForkEvent`;
- `public = true` (пастка: `public` буває `NULL` — такі рядки прибрати свідомо);
- прибрати рядки з `NULL` у `event_id`, `repo_name`, `created_at`;
- дедуп по `event_id`.

**Checkpoint:** **30 048** рядків; рівно 6 типів; усі `event_id` унікальні.
Розподіл: `PushEvent` 24 395, `PullRequestEvent` 2 172, `IssueCommentEvent` 1 408,
`WatchEvent` 1 265, `IssuesEvent` 510, `ForkEvent` 298.

---

## Крок 2 — `silver.commits` · розкриття масиву (commit grain)

Джерело: `{{ ref('events') }}`, лише `event_type = 'PushEvent'`.
`from_json(payload, PUSH_SCHEMA)` → `explode` масиву `commits` → **один рядок на коміт у push**.

**Колонки:** `commit_sha` (`commits.sha`), `repo_name`, `pushed_by` (`actor_login`),
`branch` (`ref` без префікса `refs/heads/`), `author_name`, `author_email`,
`message`, `is_distinct` (`commits.distinct`), `pushed_at` (`created_at` події),
`is_merge_commit` (`message` починається з `Merge `),
`message_subject` (перший рядок `message`), `message_length` (довжина `message`).

**Дедуп:** один коміт може прийти в кількох push-ах (force-push, кілька гілок) —
лишити **найраніший** `pushed_at` на `commit_sha` (`row_number()` по `commit_sha`).
Tie-break у `order by` **обов'язковий** (напр. `order by pushed_at, event_id`): без нього
переможець при однаковому `pushed_at` недетермінований і checkpoint «плаває» між прогонами.

**Checkpoint:** до дедупу **33 088** рядків, після — **28 237** унікальних `commit_sha`.

---

## Крок 3 — `silver.pull_requests` · остання версія стану (latest-state)

Джерело: `{{ ref('events') }}`, лише `PullRequestEvent`. `from_json(payload, PR_SCHEMA)`.

Одна PR отримує кілька подій (`opened` / `synchronize` / `closed` / `reopened`).
Грануляція вихідної таблиці: **один рядок на `(repo_name, pr_number)`** — стан із
**останньої за часом** події (`row_number()` `partition by repo_name, pr_number`
`order by event_at desc, event_id desc`, взяти `= 1`). Tie-break на `event_id` —
обов'язковий (детермінований результат).

**Колонки:** `repo_name`, `pr_number` (`number`), `title`, `author_login`
(`pull_request.user.login`), `state`, `is_merged` (`pull_request.merged`),
`is_draft`, `opened_at` (`pull_request.created_at` → timestamp),
`closed_at`, `merged_at` (timestamp, nullable),
`additions`, `deletions`, `changed_files`, `commits_count` (`pull_request.commits`),
`comments`, `review_comments`, `author_association`,
`label_names` (масив `pull_request.labels.name`),
`last_action` (`action` останньої події), `last_event_at` (`created_at` останньої події),
`churn` (`additions + deletions`),
`hours_open` (`(coalesce(closed_at, last_event_at) - opened_at)` у годинах, `double`).

**Checkpoint:** **1 959** рядків (унікальні `(repo_name, pr_number)`) з 2 172 подій.

---

## Крок 4 — `silver.issues` · злиття двох типів подій

Джерело: `{{ ref('events') }}`, типи `IssuesEvent` **та** `IssueCommentEvent`
(обидва несуть знімок `issue` у payload). `from_json(payload, ISSUE_SCHEMA)`.

Грануляція: **один рядок на `(repo_name, issue_number)`** — стан із останньої за часом
події будь-якого з двох типів (`order by event_at desc, event_id desc`).

**Колонки:** `repo_name`, `issue_number`, `title`, `author_login` (`issue.user.login`),
`state`, `opened_at` (`issue.created_at` → timestamp), `closed_at` (timestamp, nullable),
`comments` (`issue.comments` з останньої події), `label_names` (масив),
`comment_events_seen` (скільки `IssueCommentEvent` по цій issue потрапило у вибірку),
`last_event_at`, `hours_to_close`
(`(closed_at - opened_at)` у годинах, `NULL` якщо не закрита).

**Checkpoint:** **1 704** рядки з 1 918 подій (510 `IssuesEvent` + 1 408 `IssueCommentEvent`).

---

## Крок 5 — `gold.dim_repo` · conformed dimension

Джерело: `{{ ref('events') }}`. Грануляція: один рядок на репозиторій.

**Колонки:** `repo_id` (`md5(repo_name)`), `repo_name`, `repo_owner`,
`first_seen_at` (`min(created_at)`), `last_seen_at` (`max(created_at)`),
`event_count` (`count(*)`), `is_forked` (є хоч одна подія `ForkEvent` по цьому репо).

**Checkpoint:** **16 053** рядки; усі `repo_id` унікальні.

---

## Крок 6 — `gold.dim_actor` · conformed dimension

Джерело: `{{ ref('events') }}`, `actor_login IS NOT NULL`. Грануляція: один рядок на актора.

**Колонки:** `actor_id` (`md5(actor_login)`), `actor_login`,
`is_bot` (`actor_login` закінчується на `[bot]`),
`first_seen_at`, `last_seen_at`, `event_count`,
`distinct_repos` (`count(distinct repo_name)`).

**Checkpoint:** **10 514** рядків; усі `actor_id` унікальні.

---

## Крок 7 — `gold.dim_date` · згенерований календар (без seed)

Побудувати **безперервний** (gapless) календар у SQL, **не** через seed:

```sql
select explode(sequence(
    to_date('<min>'), to_date('<max>'), interval 1 day
)) as date_day
```

де `<min>` / `<max>` — мінімальна і максимальна **дата**, що фігурує як FK хоч в одному
факті: дати подій (`fact_commit.pushed_at`), а також `opened_at` / `merged_at` PR і
`opened_at` / `closed_at` issue. Порахуйте межі підзапитом по відповідних silver-моделях
(`union all` дат → `min` / `max`), не хардкодьте.

**Колонки:** `date_id` (`int`, `yyyyMMdd`), `date_day` (`date`),
`day_of_week` (1–7), `is_weekend` (`bool`), `iso_week` (`int`), `year` (`int`).

**Checkpoint:** діапазон навмисно широкий — GitHub Archive містить події про PR/issue,
створені задовго до 15.01.2024 (найраніша дата **2014-11-15**, остання **2024-01-15**).
Модель має покрити **кожен** день між `min` і `max` без пропусків — **3 349** рядків.

---

## Крок 8 — `gold.fact_commit` · fact table (grain = commit)

Джерело: `{{ ref('commits') }}`.

**Колонки:** `commit_sha`, `repo_id` (`md5(repo_name)`),
`pusher_id` (`md5(pushed_by)`), `date_id` (`yyyyMMdd` від `pushed_at`),
`branch`, `is_merge_commit`, `is_distinct`, `message_length`.

Грануляція не змінюється (1 рядок = 1 `commit_sha`). FK-колонки будуються тим самим
`md5` / `date_format`, що й ключі відповідних вимірів.

**Checkpoint:** **28 237** рядків; усі `commit_sha` унікальні.

---

## Крок 9 — `gold.fact_pull_request` · fact table (grain = PR)

Джерело: `{{ ref('pull_requests') }}`.

**Колонки:** `pr_id` (`md5(concat_ws('|', repo_name, pr_number))`), `repo_id`,
`author_id` (`md5(author_login)`),
`opened_date_id` (`yyyyMMdd` від `opened_at`),
`merged_date_id` (`yyyyMMdd` від `merged_at`, `NULL` якщо не змерджено),
`state`, `is_merged`, `is_draft`,
`additions`, `deletions`, `churn`, `changed_files`, `commits_count`,
`comments`, `review_comments`, `hours_open`, `label_count` (довжина `label_names`).

**Checkpoint:** **1 959** рядків; усі `pr_id` унікальні.

---

## Крок 10 — `gold.fact_repo_activity_daily` · багатоджерельний rollup

Найскладніша модель. Грануляція: **`(repo_id, date_id)`** — активність репозиторію за добу,
зібрана з **чотирьох** silver-джерел.

Патерн: побудувати по одному денному агрегату на джерело
(`commits`, `pull_requests`, `issues`, `events`-для-`WatchEvent`/`ForkEvent`),
звести до спільної грануляції `(repo_name, day)` і **`full outer join`** (або
`union all` + `group by`) так, щоб рядок з'являвся, якщо була активність **будь-якого**
типу. Відсутні метрики → `0`, не `NULL`.

**Колонки:** `activity_id` (`md5(concat_ws('|', repo_id, date_id))`),
`repo_id`, `date_id`,
`commits` (к-сть із `silver.commits` за `pushed_at::date`),
`distinct_committers` (`count(distinct author_email)`),
`prs_opened` (PR з `opened_at::date = day`),
`prs_merged` (PR з `merged_at::date = day`),
`issues_opened` (issue з `opened_at::date = day`),
`issues_closed` (issue з `closed_at::date = day`),
`stars` (к-сть `WatchEvent` за добу), `forks` (к-сть `ForkEvent` за добу).

> Дати беруться з різних сутностей, тож один рядок факту може поєднувати комміти за
> 15.01 і PR, відкриту 14.12 — це нормально: grain — `(repo, день)`, а не `(repo, подія)`.

**Checkpoint:** **13 403** рядки; `sum(commits)` = **28 237** (= рядків у `fact_commit`);
`sum(stars)` = **1 265**; `sum(forks)` = **298**.

---

## Тести

### schema.yml (стандартні dbt-тести)

- `not_null` + `unique` на **грануляції / surrogate key** кожної моделі:
  `events.event_id`, `commits.commit_sha`, `dim_repo.repo_id`, `dim_actor.actor_id`,
  `dim_date.date_id`, `fact_commit.commit_sha`, `fact_pull_request.pr_id`,
  `fact_repo_activity_daily.activity_id`; `not_null` на `pull_requests.pr_number`,
  `issues.issue_number`;
- `accepted_values`: `events.event_type` (6 значень),
  `pull_requests.state` (`open`, `closed`),
  `issues.state` (`open`, `closed`).

### Singular-тести (`tests/`) — бізнес-правила

| Файл | Правило |
|---|---|
| `assert_pr_churn_consistent.sql` | у `fact_pull_request` завжди `churn = additions + deletions` |
| `assert_pr_timeline_sane.sql` | немає PR, де `merged_at < opened_at` або `closed_at < opened_at` |
| `assert_daily_rollup_matches_facts.sql` | `sum(fact_repo_activity_daily.commits) = (select count(*) from fact_commit)` |

`dbt build` має пройти всі моделі **і** всі тести (зелено).

---

## Інкрементальність (перевіряється `verify.sh`)

`silver.events` — `incremental`. `verify.sh` перевіряє потік:

1. **Фаза 1:** `verify.sh` тимчасово прибирає `2024-01-15-13/14.json.gz` з `data/landing/`.
   `bronze_job.py` → `dbt build` → `silver.events` створюється (перший прогін), ≈10 000 рядків.
2. **Фаза 2:** файли повертаються. `bronze_job.py` (ідемпотентний append додає 24 000 сирих) →
   `dbt build` **без** `--full-refresh` → `silver.events` **доливає лише нові рядки**
   (`append` + `is_incremental()`-фільтр по `_ingested_at`), підсумок **30 048**.
3. **Фаза 3:** повторний прогін без нових даних → bronze пропускає, `silver.events`
   не змінюється (0 рядків додано), усі `event_id` унікальні.

Ваша `is_incremental()`-гілка має бути коректною: повторний прогін не дублює і не втрачає рядки.

---

**Definition of done:** `./verify.sh` (з кореня `homework/`) зелений — bronze ідемпотентний,
`silver.events` коректно інкрементиться до 30 048, усі checkpoint-и Silver/Gold зійшлися,
`dbt build` без помилок, singular-тести зелені.
