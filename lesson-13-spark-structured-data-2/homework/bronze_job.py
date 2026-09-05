"""Bronze — raw ingestion у таблицю bronze.raw_events. ДАНО. Не редагуйте.

Читає data/landing/*.json.gz з явною схемою. `payload` зберігається як СИРИЙ
JSON-рядок — Bronze його не парсить (контракт шару: значення не змінюються).
Розбір payload (from_json / explode) — робота Silver. Додає metadata-колонки
`_ingested_at` / `_source_file` і записує у Spark-таблицю bronze.raw_events.
Ідемпотентно: файли, що вже завантажені (за _source_file), повторно не додаються.

    uv run python bronze_job.py
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)

from common.spark import build_spark

# запускайте з кореня homework/ — шлях відносний до нього
LANDING_GLOB = "data/landing/*.json.gz"

# Явна схема читання: топ-рівень типізуємо, payload лишаємо сирим JSON-рядком
# (Spark кладе весь вкладений об'єкт у STRING as-is, коли поле оголошене StringType).
SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("actor", StructType([StructField("login", StringType())]), True),
        StructField("repo", StructType([StructField("name", StringType())]), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def main() -> None:
    spark = build_spark("l13-bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    raw = (
        spark.read.schema(SCHEMA)
        .json(LANDING_GLOB)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    table = "bronze.raw_events"
    exists = spark.catalog.tableExists(table)
    if exists:
        already = {r["_source_file"] for r in spark.table(table).select("_source_file").distinct().collect()}
        raw = raw.filter(~F.col("_source_file").isin(list(already))) if already else raw

    new_rows = raw.count()
    if new_rows == 0:
        print("Bronze: нових файлів немає — пропускаю append (ідемпотентно).")
    else:
        raw.write.mode("append" if exists else "overwrite").saveAsTable(table)
        print(f"Bronze: додано {new_rows} рядків у {table}.")

    print(f"Bronze total: {spark.table(table).count()}")
    spark.stop()


if __name__ == "__main__":
    main()
