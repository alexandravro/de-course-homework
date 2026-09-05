"""Спільний помічник для створення SparkSession — ДАНО. Не редагуйте.

Усі кроки (bronze, dbt) працюють з ОДНИМ Hive-metastore і складом таблиць у
`spark-warehouse/` поточної директорії. Тому таблицю, яку записав bronze_job, бачить
dbt (Silver + Gold моделі). Запускайте все з кореня цієї директорії.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
