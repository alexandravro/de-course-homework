"""Спільна конфігурація: Kafka, landing-зона, підключення до сховища. ДАНО.

Тільки stdlib — цей модуль імпортують і стрімінг-контейнери (там є лише
confluent-kafka), і Spark-job, і тести. Усе береться з середовища, щоб той самий
код працював і локально (`localhost:9094`), і в docker-мережі (`kafka:29092`).
"""

from __future__ import annotations

import os
from pathlib import Path

# --- Kafka -------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ride-events")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "landing-sink")
TOPIC_PARTITIONS = int(os.environ.get("TOPIC_PARTITIONS", "3"))

# --- Landing-зона ------------------------------------------------------------
LANDING_DIR = Path(os.environ.get("LANDING_DIR", "data/landing")).resolve()

# --- Сховище -----------------------------------------------------------------
PG_HOST = os.environ.get("PGHOST", "localhost")
PG_PORT = int(os.environ.get("PGPORT", "5433"))
PG_USER = os.environ.get("PGUSER", "taxi")
PG_PASSWORD = os.environ.get("PGPASSWORD", "taxi")
PG_DATABASE = os.environ.get("PGDATABASE", "taxi_dwh")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Драйвер JDBC: у docker-образі Airflow він уже лежить у /opt/spark-jars
# (див. docker/Dockerfile.airflow). Локально — тягнеться з Maven при першому
# запуску, тому перший локальний прогін bronze_job.py потребує мережі.
PG_JDBC_JAR = os.environ.get("PG_JDBC_JAR")
PG_JDBC_COORDINATES = "org.postgresql:postgresql:42.7.4"

BRONZE_TABLE = "bronze.raw_events"
