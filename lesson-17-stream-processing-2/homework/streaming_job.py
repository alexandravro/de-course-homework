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
    return StructType([
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        StructField("public", BooleanType()),
        StructField("actor", StructType([
            StructField("login", StringType()),
        ])),
        StructField("repo", StructType([
            StructField("name", StringType()),
        ])),
    ])


def read_stream(spark: SparkSession) -> DataFrame:
    return spark.readStream.schema(event_schema()).json(LANDING)


def clean_events(stream_df: DataFrame) -> DataFrame:
    return (
        stream_df
        .filter(F.col("type").isin(KEEP_TYPES))
        .filter(F.col("public") == True)
        .withColumn("event_time", F.to_timestamp("created_at"))
        .select(
            F.col("id"),
            F.col("type").alias("event_type"),
            F.col("event_time"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.name").alias("repo_name"),
        )
    )


def windowed_counts(clean_df: DataFrame) -> DataFrame:
    return (
        clean_df
        .withWatermark("event_time", WATERMARK)
        .groupBy(
            F.window("event_time", WINDOW),
            F.col("event_type"),
        )
        .count()
    )


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
        agg = windowed_counts(batch_df)
        (
            agg.select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("event_type"),
                F.col("count").alias("event_count"),
            )
            .write.mode("append")
            .parquet(OUTPUT)
        )

    (
        clean.writeStream
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


def build_summary(spark: SparkSession) -> dict:
    import polars as pl

    df = pl.read_parquet(f"{OUTPUT}/*.parquet")

    by_type = (
        df.group_by("event_type")
        .agg(pl.col("event_count").sum())
        .sort("event_type")
        .to_dicts()
    )
    by_type_dict = {row["event_type"]: row["event_count"] for row in by_type}

    total_events = df["event_count"].sum()
    n_windows = df.select(["window_start", "window_end"]).unique().height

    summary = {
        "total_events": int(total_events),
        "n_windows": int(n_windows),
        "window_seconds": 30,
        "by_type": by_type_dict,
    }

    os.makedirs(os.path.dirname(SUMMARY), exist_ok=True)
    with open(SUMMARY, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)

    return summary


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
