# Домашнє завдання L16 — Kafka producer і consumer

Ви будуєте потоковий конвеєр над реальними подіями **GitHub Archive**: producer стрімить
годину подій із мережі й публікує їх у Kafka, а consumer вичитує топік і рахує статистику.
Kafka-брокер і мережеве читання архіву — **вже готові**; ви пишете лише Python-логіку
обробки потоку.

Повна специфікація, контракти полів і розподіл балів — у [SPEC.md](SPEC.md).

## Передумови

- **Docker** + **Docker Compose** (для брокера).
- **uv** (Kafka-клієнт `confluent-kafka` уже є у спільному середовищі курсу).
- Мережа (producer тягне годину `gharchive` під час запуску).

> Усі команди виконуються **з цієї директорії** (`homework/`) — тут лежить усе потрібне:
> свій `docker-compose.yml`, свій `verify.sh`, свої `tests/`. Каталог не залежить від
> `solution/` чи кореня заняття — нічого спільного між ними немає, дублікати нормальні.

## Структура

```
homework/
├── docker-compose.yml      # дано: один Kafka broker (KRaft) на :9092
├── verify.sh                # наскрізна перевірка (свій, не спільний із solution/)
├── transform.py              # ← ВИ ТУТ ПРАЦЮЄТЕ
├── producer.py                # ← ВИ ТУТ ПРАЦЮЄТЕ
├── consumer.py                 # ← ВИ ТУТ ПРАЦЮЄТЕ
├── tests/                   # pytest: офлайн-юніти + checkpoint на stats.json
└── data/output/stats.json   # результат consumer-а (генерується, не комітиться)
```

`solution/` (еталонна реалізація) поруч, на тому самому рівні — окрема, самодостатня
директорія з такою самою структурою. Немає нічого спільного між ними (ні `common/`, ні
`tests/`, ні `docker-compose.yml`) — константи (брокер, топік, URL архіву тощо) і функція
стрімінгу `iter_archive` лежать прямо вгорі `transform.py` / `producer.py` / `consumer.py`,
позначені коментарем `# Дано, не редагувати.`

## Як запускати вручну

З цієї директорії (`homework/`):

```bash
# 1. Підняти брокер (один контейнер)
docker compose up -d

# 2. Створити топік (необов'язково — auto-create увімкнено)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic github-events \
  --partitions 3 --replication-factor 1

# 3. Запустити ваш producer (стрімить gharchive → топік)
uv run python producer.py

# 4. Запустити ваш consumer (вичитує топік → data/output/stats.json)
uv run python consumer.py

# 5. Зупинити брокер, коли завершите
docker compose down
```

## Як перевірити себе

З цієї директорії:

```bash
./verify.sh             # ваша реалізація — мета: RESULT: PASS
```

Еталон для порівняння — окремою командою з `../solution/`:

```bash
cd ../solution && ./verify.sh   # RESULT: PASS
```

`verify.sh` робить усе сам: піднімає брокер, скидає топік, запускає `producer.py` і
`consumer.py`, тоді ганяє `pytest`. На стартових заглушках перевірка очікувано **FAIL** —
вона стає **PASS**, коли ви реалізуєте всі 6 функцій.

Швидко прогнати лише офлайн-юніти (без брокера), поки пишете `transform.py`/`consumer.py`:

```bash
uv run pytest tests/test_transform.py -q
```

## Поради

- **Ключ повідомлення = `repo_name`.** Так усі події одного репозиторію потрапляють в одну
  partition і зберігають порядок per-repo. Ключ передавайте як `bytes` (`.encode("utf-8")`).
- **`producer.poll(0)`** після кожного `produce()` — не блокуюче обслуговування delivery-колбеків;
  без нього черга може переповнитись.
- **Idempotent producer:** `enable.idempotence=True` разом з `acks="all"` — ретраї без дублікатів.
- **Consumer зупиняється сам** через `IDLE_LIMIT_SECONDS` тиші. Якщо хочете перечитати топік з
  початку — змініть `group.id` або перестворіть топік (як це робить `verify.sh`).
- **`created_at` — мілісекунди (int).** Конвертацію вже дано у `_to_millis`; просто викличте її.
- Якщо брокер не піднявся — `docker compose logs kafka`. Часта причина на машинах із ~2 GB пам'яті
  для Docker — брак RAM; heap уже обмежено в `docker-compose.yml`.
