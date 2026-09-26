"""Джерело подій: публікує корпус у Kafka. ДАНО. Не редагуйте.

Це імітація продакшн-системи, якою ви не володієте: вона шле те, що шле, і полагодити
бруд у ній ви не можете — тільки обробити далі по pipeline.

Часова модель (важливо для розуміння партицій):
  * **event time** (`occurred_at`) прив'язаний до ФІКСОВАНОГО якоря `EVENT_TIME_ANCHOR`
    (за замовчуванням 2024-01-15T08:00:00Z) і не залежить від того, коли ви запустили
    стек. Тому checkpoint-числа зі SPEC.md однакові в усіх. `EVENT_TIME_ANCHOR=now`
    зсуває корпус так, що він закінчується «зараз» (для демо; checkpoint-и тоді не діють);
  * **publish time** стиснуто у `SPEED` разів: за SPEED=60 весь корпус (~2.8 год event
    time) їде в Kafka за ~2.8 хвилини. `SPEED=0` — без пауз, максимально швидко (тести).

Через це `hour=` у шляху landing-файлу (коли записали) НЕ дорівнює годині події
(коли вона сталася). Партиціювати шари за часом події — ваша робота.

Корпус публікується ОДИН раз. Якщо в топіку вже є повідомлення (перезапуск або перестворення
контейнера, повторний `docker compose up`), producer нічого не шле й засинає: інакше кожне
перестворення контейнера тихо подвоювало б дані. Опублікувати корпус ще раз свідомо
(з тими самими `event_id`: для Bronze це нові рядки, для Silver — дублікати, які має прибрати
дедуплікація):

    docker compose exec -e REPUBLISH=1 producer python -m stream.producer

Змінні середовища: SEED, TOTAL_RIDES, SPEED, EVENT_TIME_ANCHOR, REPUBLISH, KAFKA_BOOTSTRAP,
KAFKA_TOPIC.
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common import config
from stream.events import corpus_span_seconds, plan_corpus

SEED = int(os.environ.get("SEED", "42"))
TOTAL_RIDES = int(os.environ.get("TOTAL_RIDES", "1200"))
SPEED = float(os.environ.get("SPEED", "60"))
EVENT_TIME_ANCHOR = os.environ.get("EVENT_TIME_ANCHOR", "2024-01-15T08:00:00Z")
SLEEP_WHEN_DONE = os.environ.get("SLEEP_WHEN_DONE", "1") == "1"
REPUBLISH = os.environ.get("REPUBLISH", "0") == "1"


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    """Страховка для запуску поза docker compose; у compose топік створює `kafka-init`."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=20).topics:
        return
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    for name, future in futures.items():
        try:
            future.result()
            print(f"topic {name}: створено ({partitions} партиції)")
        except Exception as exc:  # топік міг зʼявитися паралельно — це не помилка
            print(f"topic {name}: {exc}")


def topic_has_messages(bootstrap: str, topic: str) -> bool:
    """Чи є в топіку хоч одне повідомлення (за офсетами всіх партицій)."""
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "producer-probe"})
    try:
        partitions = probe.list_topics(topic, timeout=20).topics[topic].partitions
        for partition in partitions:
            low, high = probe.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high > low:
                return True
        return False
    finally:
        probe.close()


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        print(f"delivery failed: {err}")


def corpus_start(span_seconds: float) -> datetime:
    if EVENT_TIME_ANCHOR == "now":
        return datetime.now(UTC) - timedelta(seconds=span_seconds)
    return datetime.fromisoformat(EVENT_TIME_ANCHOR.replace("Z", "+00:00"))


def main() -> None:
    planned = plan_corpus(SEED, TOTAL_RIDES)
    span = corpus_span_seconds(planned)
    start = corpus_start(span)
    print(
        f"корпус: {len(planned)} подій, {TOTAL_RIDES} поїздок, event time "
        f"{start:%Y-%m-%d %H:%M}Z + {span / 3600:.2f} год, SPEED={SPEED}"
    )

    ensure_topic(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC, config.TOPIC_PARTITIONS)
    if not REPUBLISH and topic_has_messages(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC):
        print(f"топік {config.KAFKA_TOPIC} уже містить повідомлення — корпус не публікую.")
        print("щоб опублікувати ще раз свідомо: REPUBLISH=1 (див. докстрінг producer.py)")
        while SLEEP_WHEN_DONE:
            time.sleep(3600)
        return
    producer = Producer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
        }
    )

    t0_wall = time.monotonic()
    for i, item in enumerate(planned, start=1):
        if SPEED > 0:
            lag = item.emit_offset_s / SPEED - (time.monotonic() - t0_wall)
            if lag > 0:
                time.sleep(lag)

        event = dict(item.event)
        occurred_at = start + timedelta(seconds=item.event_offset_s)
        event["occurred_at"] = occurred_at.isoformat().replace("+00:00", "Z")

        producer.produce(
            config.KAFKA_TOPIC,
            key=event["ride_id"].encode(),
            value=json.dumps(event, separators=(",", ":")).encode(),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        if i % 1000 == 0:
            print(f"  опубліковано {i}/{len(planned)}")

    remaining = producer.flush(60)
    if remaining:
        raise SystemExit(f"не доставлено {remaining} повідомлень")
    print(f"корпус вичерпано: {len(planned)} подій у топіку {config.KAFKA_TOPIC}")
    if SLEEP_WHEN_DONE:
        print("producer засинає; щоб опублікувати корпус ще раз — REPUBLISH=1 (див. докстрінг)")
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()
