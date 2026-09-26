"""Bronze: landing-зона -> bronze.raw_events у Postgres. ЕТАП 1 — реалізуйте три місця з TODO.

Spark у local mode читає NDJSON із landing і дописує рядки у сховище через JDBC.

Контракт шару Bronze (повністю — SPEC.md, розділ 3):
  * читаємо з ЯВНОЮ схемою (ніякого inferSchema: схема — це контракт, а не здогадка);
  * `payload` лишається СИРИМ JSON-рядком: Bronze нічого не парсить і нічого не виправляє;
  * ідемпотентність за файлом: файл, який уже завантажено, вдруге не потрапляє;
  * атомарний append: або всі нові рядки в таблиці, або жодного.

    uv run python bronze_job.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from common import config
from common.spark import build_spark

# TODO (4): явна схема конверта події: event_id, event_type, ride_id, occurred_at, source, payload.
# Підказка: `payload` — вкладений обʼєкт; якщо оголосити його як StringType, Spark віддасть
# його текст як є, без розбору.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("ride_id", StringType()),
        StructField("occurred_at", StringType()),
        StructField("source", StringType()),
        StructField("payload", StringType()),
    ]
)


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        spark.read.schema(EVENT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.ndjson")
        .json(landing_dir)
        .withColumn("_source_file", F.regexp_replace(F.input_file_name(), f"^.*{landing_dir}/", ""))
        .withColumn("occurred_at", F.to_timestamp("occurred_at"))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            "event_id",
            "event_type",
            "ride_id",
            "occurred_at",
            "source",
            "payload",
            "_source_file",
            "_ingested_at",
        )
    )


def loaded_files(spark: SparkSession) -> set[str]:
    """Файли, які вже лежать у Bronze (ключ ідемпотентності). ДАНО."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(config.JDBC_URL, query, properties=config.JDBC_PROPERTIES).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    if not already_loaded:
        return df
    from pyspark.sql import functions as F

    return df.filter(~F.col("_source_file").isin(already_loaded))


def write_bronze(df: DataFrame) -> None:
    (
        df.coalesce(1)
        .write.mode("append")
        .jdbc(config.JDBC_URL, config.BRONZE_TABLE, properties=config.JDBC_PROPERTIES)
    )


def main() -> None:
    """ДАНО."""
    spark = build_spark("bronze-job")
    try:
        raw = read_landing(spark, str(config.LANDING_DIR))
        new = select_new(raw, loaded_files(spark)).cache()

        n_new = new.count()
        if n_new == 0:
            print("Bronze: нових файлів немає — пропускаю запис (ідемпотентно).")
        else:
            n_files = new.select("_source_file").distinct().count()
            write_bronze(new)
            print(f"Bronze: додано {n_new} подій із {n_files} файлів.")

        total = spark.read.jdbc(
            config.JDBC_URL,
            f"(SELECT count(*) AS n FROM {config.BRONZE_TABLE}) AS t",
            properties=config.JDBC_PROPERTIES,
        ).collect()[0]["n"]
        print(f"Bronze total: {total}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
