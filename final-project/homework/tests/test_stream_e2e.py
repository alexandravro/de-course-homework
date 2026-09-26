"""Kafka -> consumer -> landing на живому брокері (localhost:9094). Потрібен `docker compose up -d`.

Перевіряє те, чого unit-тести не бачать: цикл читання, commit ПІСЛЯ запису, вихід по idle і
головне — що падіння consumer-а посеред роботи не губить повідомлень (at-least-once).
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tests.conftest import ROOT, require_implemented

pytestmark = pytest.mark.stream


@pytest.fixture(scope="module", autouse=True)
def _stage_is_implemented() -> None:
    require_implemented("ingest")


BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
N_MESSAGES = 600


def _make_topic() -> str:
    topic = f"test-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for future in admin.create_topics(
        [NewTopic(topic, num_partitions=3, replication_factor=1)]
    ).values():
        future.result(timeout=30)
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(N_MESSAGES):
        body = json.dumps({"event_id": f"e{i:04d}", "ride_id": f"r{i % 40}"}).encode()
        producer.produce(topic, key=f"r{i % 40}".encode(), value=body)
    assert producer.flush(30) == 0
    return topic


def _consumer_env(topic: str, group: str, landing: Path) -> dict[str, str]:
    return {
        **os.environ,
        "KAFKA_BOOTSTRAP": BOOTSTRAP,
        "KAFKA_TOPIC": topic,
        "KAFKA_GROUP_ID": group,
        "LANDING_DIR": str(landing),
        "BATCH_MAX_MESSAGES": "20",
        "BATCH_MAX_SECONDS": "1",
        "IDLE_EXIT_SECONDS": "5",
        "SESSION_TIMEOUT_MS": "6000",  # мінімум брокера: після kill -9 партиції звільняться за 6 с
    }


def _event_ids(landing: Path) -> list[str]:
    return [
        json.loads(line)["event_id"]
        for path in sorted(landing.rglob("*.ndjson"))
        for line in path.read_text().splitlines()
    ]


def _start(topic: str, group: str, landing: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "stream.consumer"],
        cwd=ROOT,
        env=_consumer_env(topic, group, landing),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def test_consumer_drains_topic_into_landing(tmp_path: Path) -> None:
    topic = _make_topic()
    proc = _start(topic, f"g-{uuid.uuid4().hex[:6]}", tmp_path)
    assert proc.wait(timeout=120) == 0

    ids = _event_ids(tmp_path)
    assert sorted(set(ids)) == [f"e{i:04d}" for i in range(N_MESSAGES)]
    assert len(ids) == N_MESSAGES, "у чистому прогоні дублікатів бути не має"
    assert list(tmp_path.rglob("*.tmp")) == []
    for path in tmp_path.rglob("*.ndjson"):
        assert path.parent.parent.name.startswith("dt=")
        assert path.parent.name.startswith("hour=")


def test_failed_write_does_not_commit_offsets(tmp_path: Path) -> None:
    """Збій запису у landing не має «з'їдати» повідомлення.

    Перший consumer не може писати (шлях landing проходить через звичайний файл), тож кожен flush
    падає. Якщо він комітить офсети ДО запису, повідомлення пропали: другий consumer стартує вже
    після них. Порядок «запис -> commit» гарантує, що другий прочитає все.
    """
    topic = _make_topic()
    group = f"g-{uuid.uuid4().hex[:6]}"
    blocker = tmp_path / "blocker"
    blocker.write_text("це файл, а не каталог")

    broken = _start(topic, group, blocker / "landing")
    try:
        broken.wait(timeout=45)  # правильний consumer падає на першому ж flush
    except subprocess.TimeoutExpired:
        broken.send_signal(signal.SIGKILL)  # або ретраїть вічно — теж припустимо
        broken.wait(timeout=10)

    good_landing = tmp_path / "landing"
    second = _start(topic, group, good_landing)
    assert second.wait(timeout=120) == 0

    ids = _event_ids(good_landing)
    assert set(ids) == {f"e{i:04d}" for i in range(N_MESSAGES)}, (
        "офсети закомічено до того, як батч потрапив у landing: повідомлення втрачено"
    )


def test_hard_kill_loses_no_messages(tmp_path: Path) -> None:
    """kill -9 посеред роботи, далі перезапуск тієї ж групи: жодне повідомлення не зникло."""
    topic = _make_topic()
    group = f"g-{uuid.uuid4().hex[:6]}"

    first = _start(topic, group, tmp_path)
    deadline = time.monotonic() + 60
    while not list(tmp_path.rglob("*.ndjson")) and time.monotonic() < deadline:
        time.sleep(0.02)
    first.send_signal(signal.SIGKILL)
    first.wait(timeout=10)

    second = _start(topic, group, tmp_path)
    assert second.wait(timeout=120) == 0

    ids = _event_ids(tmp_path)
    assert set(ids) == {f"e{i:04d}" for i in range(N_MESSAGES)}, "at-least-once порушено: є втрати"
    assert list(tmp_path.rglob("*.tmp")) == []
