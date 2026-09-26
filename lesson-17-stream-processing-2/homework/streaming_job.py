"""
Заняття 17 — Stream processing: домашнє завдання.

Реалізуйте Spark Structured Streaming job над потоком подій GitHub Archive:
файловий source -> чистка -> підрахунок подій у tumbling-вікнах за event time з
watermark -> запис у parquet через foreachBatch -> serving summary.

Заповнюйте місця, позначені `TODO`. Сигнатури функцій і шляхи міняти НЕ треба —
на них спираються тести. Запускати з каталогу homework/ (CWD = homework):

    cd homework
    uv run python streaming_job.py
    uv run pytest -q

Деталі контракту і бали — у SPEC.md.
"""

# Імпорти й окремі присвоєння — це scaffolding під TODO, тому до реалізації ruff
# бачить їх «невикористаними». Знімаємо ці попередження саме для стартового стабу.
# ruff: noqa: F401, F841

import json
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

# Шляхи — відносні до CWD = homework/ (НЕ міняти).
LANDING = "data/landing"
OUTPUT = "data/output/windowed"
CHECKPOINT = "data/checkpoints/windowed"
SUMMARY = "data/output/summary.json"

# Параметри вікна (НЕ міняти — від них залежать контрольні числа в тестах).
WINDOW = "30 seconds"
WATERMARK = "10 seconds"
KEEP_TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent", "WatchEvent"]


def build_spark() -> SparkSession:
    """Дано. UTC timezone робить межі вікон відтворюваними на будь-якій машині."""
    spark = (
        SparkSession.builder.appName("l17-streaming-homework")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def event_schema() -> StructType:
    """
    Завдання 1 (12 балів). File source НЕ виводить схему — поверніть явний StructType
    для подій gharchive. Потрібні поля: id (str), type (str), created_at (str),
    public (bool), вкладені actor.login (str), repo.name (str).
    """
    # TODO: повернути StructType([...]) із вкладеними actor/repo
    raise NotImplementedError


def read_stream(spark: SparkSession) -> DataFrame:
    """
    Завдання 2 (13 балів). Поверніть потоковий DataFrame: readStream із json-source
    над каталогом LANDING зі схемою event_schema(). Перевірка: df.isStreaming == True.
    """
    # TODO: spark.readStream.schema(...).json(LANDING)
    raise NotImplementedError


def clean_events(stream_df: DataFrame) -> DataFrame:
    """
    Завдання 3 (15 балів). Очистіть потік:
      - лишіть тільки типи з KEEP_TYPES і публічні події (public == True);
      - додайте колонку event_time = to_timestamp(created_at);
      - залиште рівно колонки: id, event_type (=type), event_time,
        actor_login (=actor.login), repo_name (=repo.name).
    """
    # TODO
    raise NotImplementedError


def windowed_counts(clean_df: DataFrame) -> DataFrame:
    """
    Завдання 4 (25 балів). Tumbling window за event_time + watermark.
    Застосуйте withWatermark(event_time, WATERMARK), згрупуйте за
    window(event_time, WINDOW) та event_type і порахуйте count().
    Поверніть DataFrame з колонками window (struct start/end), event_type, count.
    """
    # TODO
    raise NotImplementedError


def write_windows(spark: SparkSession) -> None:
    """
    Завдання 5 (20 балів). Запишіть віконні лічильники у parquet через foreachBatch
    із trigger(availableNow=True) і CHECKPOINT. У кожному батчі застосуйте
    windowed_counts(...) і допишіть (append) у OUTPUT рівно колонки:
    window_start, window_end, event_type, event_count.

    Чому foreachBatch, а не append-sink: під availableNow append+watermark не встигає
    "закрити" вікна за один прогін — foreachBatch дає детермінований скінченний вивід.
    """
    shutil.rmtree(OUTPUT, ignore_errors=True)
    shutil.rmtree(CHECKPOINT, ignore_errors=True)

    clean = clean_events(read_stream(spark))

    def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
        # TODO: agg = windowed_counts(batch_df); select 4 колонки; write append parquet -> OUTPUT
        raise NotImplementedError

    # TODO: clean.writeStream.foreachBatch(upsert_batch).option(...).trigger(...).start() та awaitTermination()
    raise NotImplementedError


def build_summary(spark: SparkSession) -> dict:
    """
    Завдання 6 (15 балів). Serving layer: прочитайте OUTPUT батчем і складіть зведення:
      {"total_events": int, "n_windows": int, "window_seconds": 30, "by_type": {type: int}}
    Запишіть його у SUMMARY (json, indent=2, sort_keys=True) і поверніть як dict.
    """
    # TODO
    raise NotImplementedError


def main() -> None:
    spark = build_spark()
    try:
        write_windows(spark)
        summary = build_summary(spark)
        print("SUMMARY:", json.dumps(summary, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
