# Завдання 4-6: Kafka consumer.
# Запуск із цієї директорії (homework/):  uv run python consumer.py
import json
import os

from confluent_kafka import Consumer
from icecream import ic

# Дано, не редагувати.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "github-events"
OUTPUT_PATH = "data/output/stats.json"

GROUP_ID = "github-stats-consumer"
IDLE_LIMIT_SECONDS = 5.0  # зупинитись, коли топік мовчить стільки секунд


def update_counts(by_type: dict, by_repo: dict, event: dict) -> None:
    event_type = event["event_type"]
    repo_name = event["repo_name"]
    by_type[event_type] = by_type.get(event_type, 0) + 1
    by_repo[repo_name] = by_repo.get(repo_name, 0) + 1
   


def top_repos(by_repo: dict, n: int = 5) -> list:
    sorted_repos = sorted(by_repo.items(), key=lambda x: (-x[1], x[0]))
    return [[name, count] for name, count in sorted_repos[:n]]


def run_consumer() -> dict:
    import time
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    by_type, by_repo = {}, {}
    total = 0
    idle_since = None

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if idle_since is None:
                idle_since = time.time()
            elif time.time() - idle_since >= IDLE_LIMIT_SECONDS:
                break
            continue
        if msg.error():
            continue
        idle_since = None
        event = json.loads(msg.value())
        update_counts(by_type, by_repo, event)
        total += 1

    consumer.close()

    stats = {
        "total": total,
        "by_type": by_type,
        "top_repos": top_repos(by_repo, 5),
    }
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(stats, f, indent=2)
    return stats


if __name__ == "__main__":
    ic(run_consumer())
