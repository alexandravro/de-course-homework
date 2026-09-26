# Завдання 3: Kafka producer.
# Запуск із цієї директорії (homework/):  uv run python producer.py
import gzip
import json
import urllib.request
from typing import Iterator

from confluent_kafka import Producer
from icecream import ic

from transform import event_filter, flatten_event

# Дано, не редагувати.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "github-events"
ARCHIVE_URL = "https://data.gharchive.org/2024-01-15-14.json.gz"
MAX_RAW = 100_000

# gharchive returns HTTP 403 to urllib's default User-Agent, so set our own.
_USER_AGENT = "de-course-homework/1.0"


def iter_archive(url: str, max_raw: int) -> Iterator[dict]:
    """Дано, не редагувати.

    Yield up to `max_raw` raw GitHub Archive events (parsed JSON dicts).
    Records arrive in the file's original (roughly chronological) order. No
    filtering or flattening happens here — that is your job in transform.py.
    """
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            for count, line in enumerate(gz):
                if count >= max_raw:
                    break
                yield json.loads(line)


def build_producer() -> Producer:
    """Завдання 3a.

    Поверніть налаштований confluent_kafka.Producer, що під'єднується до
    BOOTSTRAP_SERVERS. Увімкніть idempotent producer (`enable.idempotence`) і
    `acks="all"`, щоб ретраї не створювали дублікатів.
    """
    raise NotImplementedError("Реалізуйте build_producer")


def run_producer() -> int:
    """Завдання 3b (разом 25 балів).

    1. Створіть producer через build_producer().
    2. Пройдіть події з iter_archive(ARCHIVE_URL, MAX_RAW).
    3. Відкиньте ті, що не проходять event_filter().
    4. Для решти: flatten_event(), потім produce у топік TOPIC,
       де key = repo_name (bytes), value = JSON-байти запису.
       Ключ за repo_name тримає події одного репозиторію в одній partition.
    5. Після кожного produce() викликайте producer.poll(0) (не блокуюче).
    6. Наприкінці producer.flush(30). Поверніть к-сть надісланих подій.
    """
    raise NotImplementedError("Реалізуйте run_producer")


if __name__ == "__main__":
    ic(run_producer())
