"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile. ЕТАП 3 — створіть DAG за SPEC.md, розділ 5.

Контейнер Airflow має Java, pyspark, JDBC-драйвер і dbt (в окремому venv, див.
docker/Dockerfile.airflow). Проєкт змонтовано в /opt/airflow/project.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"  # dbt у окремому venv
# target/ і logs/ пишемо в /tmp контейнера, щоб не смітити у змонтованому проєкті.
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="rides_medallion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rides", "medallion"],
) as dag:
    bronze_spark = BashOperator(
        task_id="bronze_spark",
        bash_command=f"cd {PROJECT} && python bronze_job.py",
    )

    bronze_contract = BashOperator(
        task_id="bronze_contract",
        bash_command=f"{DBT} test {DBT_DIRS} --select source:bronze --indirect-selection cautious",
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"{DBT} build {DBT_DIRS} --selector silver --indirect-selection cautious",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"{DBT} build {DBT_DIRS} --selector gold --indirect-selection cautious",
    )

    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{DBT} test {DBT_DIRS} --selector reconcile --indirect-selection cautious",
    )

    bronze_spark >> bronze_contract >> silver >> gold >> reconcile
