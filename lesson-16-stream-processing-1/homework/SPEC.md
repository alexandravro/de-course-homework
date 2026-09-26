# L16 — Специфікація домашнього завдання

Побудувати **Kafka producer і consumer** над реальним потоком подій **GitHub Archive**.
Брокер (один Kafka у Docker) і мережеве читання архіву — **дано**. Ви пишете лише логіку
потокової обробки: фільтрацію, розпластання, публікацію і агрегацію.

## Дані

- Джерело: `https://data.gharchive.org/2024-01-15-14.json.gz` (immutable година подій).
- Функція `iter_archive` (дана вгорі `producer.py`) стрімить цей файл із мережі **порціями**
  (не завантажує цілком) і віддає перші `MAX_RAW = 100 000` сирих подій як `dict`.
- Серіалізація в Kafka — **звичайний JSON** (без Avro і Schema Registry; стек навмисно мінімальний).

## Що дано (не редагувати)

| Файл | Призначення |
|---|---|
| `docker-compose.yml` | один Kafka broker (KRaft) на `localhost:9092` |
| Константи вгорі `transform.py` / `producer.py` / `consumer.py` | брокер, топік, URL архіву, 5 типів подій, шлях виводу — позначені `# Дано, не редагувати.` |
| `iter_archive(url, max_raw)` вгорі `producer.py` | стрімінг архіву з мережі — теж дано |

## Що реалізувати

Редагуєте файли у `homework/`. Ваша реалізація має проходити ті самі перевірки, що й
еталон у `solution/`: `./verify.sh` (з директорії `homework/`) має стати **PASS**.

| # | Файл / функція | Що зробити | Балів |
|---|---|---|---:|
| 1 | `transform.py` → `flatten_event` | вкладена подія → плоский запис (9 полів, payload-поля nullable) | 18 |
| 2 | `transform.py` → `event_filter` | лишити тільки 5 типів і публічні події | 12 |
| 3 | `producer.py` → `build_producer` + `run_producer` | idempotent producer; стрім → фільтр → flatten → produce (key = `repo_name`, value = JSON) | 25 |
| 4 | `consumer.py` → `update_counts` | бігучі лічильники per-type і per-repo | 15 |
| 5 | `consumer.py` → `top_repos` | топ-N репозиторіїв як `[name, count]`, спадно, тай-брейк за іменем | 10 |
| 6 | `consumer.py` → `run_consumer` | прочитати топік, зібрати `stats`, записати `data/output/stats.json` | 20 |
| | | **Разом** | **100** |

## Контракт `flatten_event`

Плоский запис мусить містити рівно ці ключі:

| Ключ | Тип | Джерело |
|---|---|---|
| `id` | str | `event["id"]` |
| `event_type` | str | `event["type"]` |
| `created_at` | int | epoch-**мілісекунди** з `event["created_at"]` (готова `_to_millis`) |
| `actor_login` | str | `event["actor"]["login"]` |
| `repo_name` | str | `event["repo"]["name"]` |
| `public` | bool | `event.get("public", True)` |
| `payload_action` | str \| None | `payload.get("action")` |
| `payload_ref` | str \| None | `payload.get("ref")` |
| `payload_commit_count` | int \| None | `len(payload["commits"])` якщо є, інакше None |

## Контрольні числа (checkpoint)

Для зафіксованої години та `MAX_RAW = 100 000`:

```
total = 79 646
by_type = PushEvent 62 871 · PullRequestEvent 7 136 · IssueCommentEvent 4 421
          · WatchEvent 3 505 · IssuesEvent 1 713
top repo = LMAO-armv8/kernel_samsung_r0q (1 182)
```

`data/output/stats.json` має точно відтворити ці числа (перевіряє `tests/test_output.py`).

## Перевірка

З директорії `homework/`:

```bash
./verify.sh             # ваша реалізація: має стати PASS
```

Еталон — окремою командою з `solution/`:

```bash
cd ../solution && ./verify.sh   # PASS
```

Кожен `verify.sh` — самодостатній: піднімає свій брокер, скидає топік, запускає свої
`producer.py`/`consumer.py`, далі `pytest` (офлайн-юніти + checkpoint на `stats.json`).
