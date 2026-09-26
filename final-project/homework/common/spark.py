"""Локальна SparkSession для bronze-job. ДАНО. Не редагуйте.

Spark працює в local mode: кластера немає, драйвер і екзекʼютори — це один
процес на вашому ноутбуці (або всередині контейнера Airflow).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from common import config


def build_spark(app_name: str = "final-project") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # UTC скрізь: інакше межі годинних партицій залежали б від таймзони ноутбука.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if config.PG_JDBC_JAR:
        builder = builder.config("spark.jars", config.PG_JDBC_JAR)
    else:
        builder = builder.config("spark.jars.packages", config.PG_JDBC_COORDINATES)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
