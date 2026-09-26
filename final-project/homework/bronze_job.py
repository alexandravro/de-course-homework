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
from pyspark.sql.types import StructType

from common import config
from common.spark import build_spark

# TODO (4): явна схема конверта події: event_id, event_type, ride_id, occurred_at, source, payload.
# Підказка: `payload` — вкладений обʼєкт; якщо оголосити його як StringType, Spark віддасть
# його текст як є, без розбору.
EVENT_SCHEMA = StructType([])


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Сирі події з landing плюс метадані ingestion.

    TODO (4): прочитайте `*.ndjson` з `landing_dir` (рекурсивно; `*.ndjson.tmp` — це in-flight
    файли consumer-а, їх читати не можна) з `EVENT_SCHEMA`, і поверніть колонки:
      event_id, event_type, ride_id, occurred_at (timestamp), source, payload (СИРИЙ текст),
      _source_file (шлях ВІДНОСНО landing/, напр. `dt=…/hour=…/part-….ndjson`),
      _ingested_at (current_timestamp).
    `dt=` і `hour=` у шляху — час запису файлу, а не бізнес-колонки: колонками стати не мають.
    """
    raise NotImplementedError("TODO (4): read_landing")


def loaded_files(spark: SparkSession) -> set[str]:
    """Файли, які вже лежать у Bronze (ключ ідемпотентності). ДАНО."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(config.JDBC_URL, query, properties=config.JDBC_PROPERTIES).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    """Лишає рядки з файлів, яких ще нема в Bronze.

    TODO (5): відфільтруйте за `_source_file`. Порожній `already_loaded` — це перший запуск.
    """
    raise NotImplementedError("TODO (5): select_new")


def write_bronze(df: DataFrame) -> None:
    """Append у `config.BRONZE_TABLE` ОДНІЄЮ транзакцією.

    TODO (6): запишіть через JDBC (`config.JDBC_URL`, `config.JDBC_PROPERTIES`) у режимі append.
    Вимога — «або все, або нічого»: збій посеред запису не лишає в таблиці частини батча.
    Подумайте, скільки транзакцій відкриває Spark при JDBC-записі й від чого це залежить.
    """
    raise NotImplementedError("TODO (6): write_bronze")


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
