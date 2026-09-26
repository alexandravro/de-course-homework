# L17 — Специфікація домашнього завдання

Реалізувати **Spark Structured Streaming** job, що рахує події **GitHub Archive** у
часових вікнах за **event time** з **watermark** і пише результат у parquet.

Брокера тут немає (це заняття про *processing*, не про event brokers — той бік був у
L16). Source — **файловий**: каталог `data/landing/` із зафіксованою годиною подій.

## Дані

- `data/landing/2024-01-15-12.json.gz` — реальний семпл gharchive (перші **12 000**
  сирих подій години `2024-01-15-12`, immutable). Закомічений у репозиторій, мережа не
  потрібна. Часовий діапазон подій: `12:00:00`–`12:02:49` UTC.
- Серіалізація — звичайний JSON (по одному об'єкту на рядок), як у gharchive.

## Що дано (не редагувати)

| Елемент | Призначення |
|---|---|
| `build_spark()` | локальна `SparkSession`, `spark.sql.session.timeZone=UTC` (відтворювані межі вікон) |
| Константи `LANDING / OUTPUT / CHECKPOINT / SUMMARY` | шляхи виводу |
| Константи `WINDOW="30 seconds" / WATERMARK="10 seconds" / KEEP_TYPES` | параметри вікна |
| `data/landing/...json.gz` | вхідний семпл |
| `tests/test_output.py` | перевірки на згенерованих даних |
| `verify.sh` | прогін job + pytest |

## Що реалізувати

Заповнюєте `TODO` у `streaming_job.py`. `./verify.sh tasks` має стати **PASS** (=
еквівалентно `./verify.sh solution`).

| # | Функція | Що зробити | Балів |
|---|---|---|---:|
| 1 | `event_schema` | явний `StructType` для подій (file source не виводить схему): `id`, `type`, `created_at`, `public`, вкладені `actor.login`, `repo.name` | 12 |
| 2 | `read_stream` | `spark.readStream.schema(...).json(LANDING)` — потоковий DataFrame (`isStreaming == True`) | 13 |
| 3 | `clean_events` | лишити `KEEP_TYPES` і публічні події; `event_time = to_timestamp(created_at)`; спроєктувати 5 колонок | 15 |
| 4 | `windowed_counts` | `withWatermark` + `groupBy(window(event_time, WINDOW), event_type).count()` | 25 |
| 5 | `write_windows` | `foreachBatch` + `trigger(availableNow=True)` + checkpoint; append рівно 4 колонок у `OUTPUT` | 20 |
| 6 | `build_summary` | прочитати `OUTPUT` батчем, скласти `summary.json` | 15 |
| | | **Разом** | **100** |

## Контракт виводу `OUTPUT` (parquet)

Рівно ці колонки:

| Колонка | Тип | Джерело |
|---|---|---|
| `window_start` | timestamp | `window.start` |
| `window_end` | timestamp | `window.end` |
| `event_type` | str | тип події |
| `event_count` | long | `count()` у вікні |

## Контракт `summary.json`

```json
{
  "total_events": <int>,
  "n_windows": <int>,
  "window_seconds": 30,
  "by_type": {"PushEvent": <int>, "...": <int>}
}
```

## Чому `foreachBatch`, а не append-sink

Під `trigger(availableNow=True)` зв'язка append + watermark не встигає «закрити» вікна
за один прогін (watermark просувається лише між мікро-батчами, а вхід уже вичерпано) —
вивід виходить порожнім. `foreachBatch` обробляє скінченний потік і дає **детермінований**
результат. Watermark лишаємо як у проді — він обмежує state для late data.

## Контрольні числа (checkpoint)

Для семплу `2024-01-15-12` (12 000 подій), `WINDOW=30s`, UTC:

```
total_events = 9 785        n_windows = 6
by_type = PushEvent 7 977 · PullRequestEvent 783 · IssueCommentEvent 490
          · WatchEvent 373 · IssuesEvent 162
вікна (UTC) = 12:00:00→1 748 · 12:00:30→1 755 · 12:01:00→1 727
            · 12:01:30→1 746 · 12:02:00→1 750 · 12:02:30→1 059
перше вікно, PushEvent = 1 468
```
