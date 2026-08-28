"""Фікстури тестів L12.

Два набори перевірок:

* `test_functions.py` — unit-тести ваших функцій на крихітних DataFrame у пам'яті.
  Швидкі, запускати job не треба. Кожна фікстура зібрана вручну, тому кожна
  функція перевіряється **незалежно** від решти: зламаний `flatten` не валить
  тести `owner_totals`.
* `test_outputs.py` — приймальні перевірки того, що job записав у `data/output/`.
  Спочатку запустіть `uv run python job.py`.

У фікстурах навмисно є «брудні» рядки, яких немає у справжніх landing-файлах:
дублікат, NULL у `public`, відсутній `repo`, NULL у `actor.login`.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import polars as pl
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Фіксуємо UTC і в Python, і в Spark — інакше date_trunc("hour") дасть різні
# результати на машинах у різних часових поясах.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

OUTPUT_DIR = "data/output"

# Схема landing — та сама, яку має повернути ваш event_schema()
LANDING_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("actor", StructType([StructField("login", StringType())])),
        StructField("repo", StructType([StructField("name", StringType())])),
        StructField("public", BooleanType()),
        StructField("created_at", StringType()),
    ]
)

FLAT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("actor_login", StringType()),
        StructField("repo_name", StringType()),
        StructField("public", BooleanType()),
        StructField("created_at", TimestampType()),
    ]
)

EVENTS_SCHEMA = StructType(
    [
        *FLAT_SCHEMA.fields,
        StructField("repo_owner", StringType()),
        StructField("is_bot", BooleanType()),
        StructField("hour", TimestampType()),
    ]
)


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 15, hour, minute)


# 8 сирих подій: дубль, нецільовий тип і три різні NULL-и
RAW_ROWS = [
    ("1", "PushEvent", ("alice",), ("acme/api",), True, "2024-01-15T12:00:00Z"),
    ("1", "PushEvent", ("alice",), ("acme/api",), True, "2024-01-15T12:00:00Z"),
    ("2", "WatchEvent", ("bob",), ("acme/web",), True, "2024-01-15T12:30:00Z"),
    ("3", "IssuesEvent", ("ci[bot]",), ("globex/lib",), True, "2024-01-15T13:10:00Z"),
    ("4", "ForkEvent", ("carol",), ("acme/api",), True, "2024-01-15T13:20:00Z"),
    ("5", "PushEvent", ("dave",), None, True, "2024-01-15T13:30:00Z"),
    ("6", "PushEvent", ("erin",), ("globex/lib",), None, "2024-01-15T13:40:00Z"),
    ("7", "PushEvent", (None,), ("globex/cli",), True, "2024-01-15T14:00:00Z"),
]

FLAT_ROWS = [
    ("1", "PushEvent", "alice", "acme/api", True, _ts(12)),
    ("1", "PushEvent", "alice", "acme/api", True, _ts(12)),
    ("2", "WatchEvent", "bob", "acme/web", True, _ts(12, 30)),
    ("3", "IssuesEvent", "ci[bot]", "globex/lib", True, _ts(13, 10)),
    ("4", "ForkEvent", "carol", "acme/api", True, _ts(13, 20)),
    ("5", "PushEvent", "dave", None, True, _ts(13, 30)),
    ("6", "PushEvent", "erin", "globex/lib", None, _ts(13, 40)),
    ("7", "PushEvent", None, "globex/cli", True, _ts(14)),
]

# те, що лишається після clean() + with_derived()
EVENTS_ROWS = [
    ("1", "PushEvent", "alice", "acme/api", True, _ts(12), "acme", False, _ts(12)),
    ("2", "WatchEvent", "bob", "acme/web", True, _ts(12, 30), "acme", False, _ts(12)),
    ("3", "IssuesEvent", "ci[bot]", "globex/lib", True, _ts(13, 10), "globex", True, _ts(13)),
    ("7", "PushEvent", None, "globex/cli", True, _ts(14), "globex", False, _ts(14)),
]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("l12-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw(spark: SparkSession):
    """Сирі події — так, як їх повертає read_raw()."""
    return spark.createDataFrame(RAW_ROWS, schema=LANDING_SCHEMA)


@pytest.fixture(scope="session")
def flat(spark: SparkSession):
    """Очікуваний результат flatten() — вхід для clean()."""
    return spark.createDataFrame(FLAT_ROWS, schema=FLAT_SCHEMA)


@pytest.fixture(scope="session")
def events(spark: SparkSession):
    """Очікуваний результат clean() + with_derived() — вхід для marts."""
    return spark.createDataFrame(EVENTS_ROWS, schema=EVENTS_SCHEMA)


# ── читання артефактів job-а (для test_outputs.py) ───────────────────────────
def _require(path: str) -> str:
    if not os.path.exists(path):
        pytest.fail(
            f"Очікуваний артефакт відсутній: {path}\n"
            f"Спочатку запустіть job: uv run python job.py"
        )
    return path


def _read(name: str) -> pl.DataFrame:
    return pl.read_parquet(f"{_require(f'{OUTPUT_DIR}/{name}')}/*.parquet")


@pytest.fixture(scope="session")
def out_events() -> pl.DataFrame:
    _require(f"{OUTPUT_DIR}/events")
    return pl.read_parquet(f"{OUTPUT_DIR}/events/**/*.parquet", hive_partitioning=True)


@pytest.fixture(scope="session")
def out_owner_totals() -> pl.DataFrame:
    return _read("owner_totals")


@pytest.fixture(scope="session")
def out_top_repos() -> pl.DataFrame:
    return _read("top_repos")


@pytest.fixture(scope="session")
def out_summary() -> pl.DataFrame:
    return _read("summary")
