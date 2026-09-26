"""Bronze на фікстурах: Spark (local mode) + Postgres. Потрібен `docker compose up -d`.

Перші тести гонять функції `bronze_job` в одному процесі з локальною SparkSession, решта —
запускають `bronze_job.py` як окремий процес проти Postgres, як це робитиме Airflow.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from pyspark.sql import SparkSession

from bronze_job import read_landing, select_new
from scripts import reset_warehouse
from tests.conftest import FIXTURES, ROOT, copy_batches, count_lines, require_implemented

pytestmark = pytest.mark.warehouse


@pytest.fixture(scope="module", autouse=True)
def _stage_is_implemented() -> None:
    require_implemented("ingest")


POISON = {
    "event_id": "ev-poison",
    "event_type": "ride_requested",
    "ride_id": "r-99999",
    "source": "rider_app",
    "payload": {},
    "occurred_at": "2024-01-15T09:00:00Z",
}


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    from common.spark import build_spark

    session = build_spark("tests")
    yield session
    session.stop()


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    """Каталог, названий `landing`: `_source_file` рахується відносно нього."""
    path = tmp_path / "landing"
    path.mkdir()
    return path


def _fixture_events(*batches: int) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for batch in batches
        for path in sorted((FIXTURES / f"batch_{batch}").rglob("*.ndjson"))
        for line in path.read_text().splitlines()
    ]


def _canon(payload: object) -> str:
    return json.dumps(payload, sort_keys=True)


def run_bronze(landing: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "bronze_job.py"],
        cwd=ROOT,
        env={**os.environ, "LANDING_DIR": str(landing)},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


def bronze_count(pg: psycopg.Connection) -> int:
    row = pg.execute("select count(*) from bronze.raw_events").fetchone()
    assert row is not None
    return int(row[0])


# --- read_landing / select_new: один процес, без Postgres -------------------------------------


def test_read_landing_columns_and_types(spark: SparkSession, landing: Path) -> None:
    copy_batches(landing, 1)
    df = read_landing(spark, str(landing))
    assert dict(df.dtypes) == {
        "event_id": "string",
        "event_type": "string",
        "ride_id": "string",
        "occurred_at": "timestamp",
        "source": "string",
        "payload": "string",
        "_source_file": "string",
        "_ingested_at": "timestamp",
    }
    assert df.count() == count_lines(1)


def test_dt_and_hour_from_the_path_are_not_columns(spark: SparkSession, landing: Path) -> None:
    copy_batches(landing, 1)
    columns = set(read_landing(spark, str(landing)).columns)
    assert not {"dt", "hour"} & columns


def test_payload_is_raw_json_text(spark: SparkSession, landing: Path) -> None:
    copy_batches(landing, 1, 2)
    rows = read_landing(spark, str(landing)).select("event_id", "payload").collect()
    got = Counter((r["event_id"], _canon(json.loads(r["payload"]))) for r in rows)
    want = Counter((e["event_id"], _canon(e["payload"])) for e in _fixture_events(1, 2))
    assert got == want
    assert all(isinstance(r["payload"], str) and r["payload"].startswith("{") for r in rows)


def test_source_file_is_relative_to_landing(spark: SparkSession, landing: Path) -> None:
    copy_batches(landing, 1)
    files = {
        r[0] for r in read_landing(spark, str(landing)).select("_source_file").distinct().collect()
    }
    assert len(files) == 3
    for name in files:
        assert name.startswith("dt=2024-01-15/hour="), name
        assert name.endswith(".ndjson")
        assert "file:" not in name and "landing" not in name


def test_in_flight_tmp_files_are_ignored(spark: SparkSession, landing: Path) -> None:
    copy_batches(landing, 1)
    tmp = landing / "dt=2024-01-15" / "hour=09" / "part-p0-o000000009000-o000000009001.ndjson.tmp"
    tmp.write_text("це напівзаписаний файл, а не JSON\n")
    assert read_landing(spark, str(landing)).count() == count_lines(1)


def test_select_new_skips_already_loaded_files(spark: SparkSession, landing: Path) -> None:
    copy_batches(landing, 1)
    df = read_landing(spark, str(landing))
    files = sorted(r[0] for r in df.select("_source_file").distinct().collect())
    skipped = files[0]
    n_skipped = df.filter(df["_source_file"] == skipped).count()
    assert select_new(df, set()).count() == df.count()
    assert select_new(df, {skipped}).count() == df.count() - n_skipped
    assert select_new(df, set(files)).count() == 0


# --- bronze_job.py як окремий процес проти Postgres -----------------------------------------------


def test_bronze_job_is_idempotent_and_incremental(pg: psycopg.Connection, landing: Path) -> None:
    reset_warehouse.main()

    copy_batches(landing, 1, 2)
    first = run_bronze(landing)
    assert first.returncode == 0, first.stderr[-2000:]
    assert bronze_count(pg) == count_lines(1, 2)

    again = run_bronze(landing)
    assert again.returncode == 0, again.stderr[-2000:]
    assert bronze_count(pg) == count_lines(1, 2), "повторний запуск без нових файлів щось дописав"

    # Нові файли + in-flight .tmp: дописуються лише нові, .tmp ігнорується.
    copy_batches(landing, 3, 4)
    (landing / "dt=2024-01-15" / "hour=12").mkdir(parents=True, exist_ok=True)
    (landing / "dt=2024-01-15" / "hour=12" / "part-p0-o1-o2.ndjson.tmp").write_text("сміття\n")
    third = run_bronze(landing)
    assert third.returncode == 0, third.stderr[-2000:]
    assert bronze_count(pg) == count_lines(1, 2, 3, 4)

    # Один запис — один `_ingested_at`; старі рядки не чіпали.
    runs = pg.execute("select count(distinct _ingested_at) from bronze.raw_events").fetchone()
    assert runs is not None and runs[0] == 2
    per_file: dict[str, int] = {
        r[0]: r[1]
        for r in pg.execute(
            "select _source_file, count(*) from bronze.raw_events group by 1"
        ).fetchall()
    }
    for path in (FIXTURES).rglob("*.ndjson"):
        rel = path.relative_to(path.parents[2]).as_posix()  # dt=…/hour=…/part-….ndjson
        assert per_file[rel] == sum(1 for _ in path.open()), rel

    # Payload дійшов до Postgres без втрат.
    rows = pg.execute("select event_id, payload from bronze.raw_events").fetchall()
    got = Counter((r[0], _canon(json.loads(r[1]))) for r in rows)
    want = Counter((e["event_id"], _canon(e["payload"])) for e in _fixture_events(1, 2, 3, 4))
    assert got == want


def test_failed_write_leaves_no_partial_batch_and_retry_recovers(
    pg: psycopg.Connection, landing: Path
) -> None:
    """Збій посеред запису не лишає в Bronze частини батча; повторний запуск донавантажує все.

    Отруйний рядок порушує CHECK-обмеження. Якби запис ішов кількома транзакціями (по одній
    на партицію Spark), партиції, що встигли закомітитись, лишилися б у таблиці, а їхні файли
    повторний запуск вважав би завантаженими й пропустив.
    """
    reset_warehouse.main()
    copy_batches(landing, 1)
    poison_dir = landing / "dt=2024-01-15" / "hour=99"
    poison_dir.mkdir(parents=True)
    poison_file = poison_dir / "part-p9-o000000000000-o000000000000.ndjson"
    poison_file.write_text(json.dumps(POISON) + "\n")

    pg.execute(
        "ALTER TABLE bronze.raw_events ADD CONSTRAINT no_poison CHECK (event_id <> 'ev-poison')"
    )
    try:
        failed = run_bronze(landing)
        assert failed.returncode != 0, "job мав впасти на отруйному рядку"
        assert bronze_count(pg) == 0, "збій лишив у Bronze напівзавантажений батч"
    finally:
        pg.execute("ALTER TABLE bronze.raw_events DROP CONSTRAINT no_poison")

    poison_file.unlink()
    retry = run_bronze(landing)
    assert retry.returncode == 0, retry.stderr[-2000:]
    assert bronze_count(pg) == count_lines(1)
