"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile. ЕТАП 3 — створіть DAG за SPEC.md, розділ 5.

Контейнер Airflow має Java, pyspark, JDBC-драйвер і dbt (в окремому venv, див.
docker/Dockerfile.airflow). Проєкт змонтовано в /opt/airflow/project.
"""

from __future__ import annotations

from datetime import datetime, timedelta  # noqa: F401 — знадобляться вам

from airflow import DAG  # noqa: F401
from airflow.operators.bash import BashOperator  # noqa: F401

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"  # dbt у окремому venv
# target/ і logs/ пишемо в /tmp контейнера, щоб не смітити у змонтованому проєкті.
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

# TODO: DAG `rides_medallion` (розклад, catchup, max_active_runs, default_args) і п'ять
# BashOperator-задач у порядку bronze_spark >> bronze_contract >> silver >> gold >> reconcile.
# Що саме запускає кожна задача і які параметри потрібні DAG-у — у SPEC.md, розділ 5.
# Перевірка: ./verify.sh orchestrate
