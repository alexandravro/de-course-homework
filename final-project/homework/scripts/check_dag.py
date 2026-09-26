"""Перевірка структури DAG-у `rides_medallion`. ДАНО.

Запускається ВСЕРЕДИНІ контейнера Airflow (локально Airflow не встановлено):

    docker compose --profile airflow exec -T airflow-scheduler \
        python /opt/airflow/project/scripts/check_dag.py
"""

from __future__ import annotations

import sys
from itertools import pairwise

from airflow.models import DagBag
from airflow.operators.bash import BashOperator

DAG_ID = "rides_medallion"
CHAIN = ["bronze_spark", "bronze_contract", "silver", "gold", "reconcile"]
# task_id -> фрагменти, які мають бути в bash_command
COMMANDS = {
    "bronze_spark": ["bronze_job.py"],
    "bronze_contract": [" test ", "source:bronze"],
    "silver": [" build ", "--selector silver", "--indirect-selection cautious"],
    "gold": [" build ", "--selector gold", "--indirect-selection cautious"],
    "reconcile": [" test ", "--selector reconcile"],
}

failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("  ok    " if ok else "  FAIL  ") + message)
    if not ok:
        failures.append(message)


def main() -> int:
    bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    check(not bag.import_errors, f"DAG імпортується без помилок {dict(bag.import_errors) or ''}")
    dag = bag.get_dag(DAG_ID)
    check(dag is not None, f"DAG {DAG_ID} існує")
    if dag is None:
        return 1

    check(
        dag.schedule_interval == "*/5 * * * *", f"розклад кожні 5 хвилин ({dag.schedule_interval})"
    )
    check(dag.catchup is False, "catchup=False")
    check(dag.max_active_runs == 1, f"max_active_runs=1 ({dag.max_active_runs})")

    args = dag.default_args
    check(int(args.get("retries", 0)) >= 1, f"retries >= 1 ({args.get('retries')})")
    check("retry_delay" in args, "заданий retry_delay")
    check("execution_timeout" in args, "заданий execution_timeout")

    check(set(dag.task_ids) == set(CHAIN), f"рівно п'ять задач {sorted(dag.task_ids)}")
    for upstream, downstream in pairwise(CHAIN):
        if upstream in dag.task_ids and downstream in dag.task_ids:
            got = {t.task_id for t in dag.get_task(upstream).downstream_list}
            check(got == {downstream}, f"{upstream} >> {downstream} (downstream: {sorted(got)})")

    for task_id, fragments in COMMANDS.items():
        if task_id not in dag.task_ids:
            continue
        task = dag.get_task(task_id)
        command = task.bash_command if isinstance(task, BashOperator) else ""
        for fragment in fragments:
            check(fragment in f" {command} ", f"{task_id}: команда містить {fragment!r}")
        if task_id != "bronze_spark":
            check(
                "tag:" not in command, f"{task_id}: селектор із selectors.yml, а не --select tag:"
            )

    print("PASS ✅  структура DAG-у відповідає SPEC" if not failures else "FAIL ❌")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
