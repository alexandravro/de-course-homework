"""Кожен singular-тест dbt має виконуватися хоча б одним кроком DAG-у. Postgres не потрібен.

Під `--indirect-selection cautious` тест запускається лише тоді, коли в прогоні є ВСІ його
parent-моделі. Тест, що посилається на моделі різних шарів (наприклад, `events` із Silver і
`dim_driver` із Gold), не запуститься ні в `silver`, ні в `gold` — і мовчки нічого не
перевірятиме. Такому тесту потрібен тег `reconcile`: цей крок вибирає тести за тегом напряму.
"""

from __future__ import annotations

from tests.conftest import ROOT, run_dbt

STEPS = {
    "silver": ["--selector", "silver", "--indirect-selection", "cautious"],
    "gold": ["--selector", "gold", "--indirect-selection", "cautious"],
    "reconcile": ["--selector", "reconcile"],
}


def _tests_run_by(step_args: list[str]) -> set[str]:
    result = run_dbt("ls", "--resource-type", "test", "--output", "name", *step_args)
    assert result.returncode == 0, result.stdout[-3000:]
    return {line.strip() for line in result.stdout.splitlines() if line.startswith("assert_")}


def test_every_singular_test_is_run_by_some_dag_step() -> None:
    files = {path.stem for path in (ROOT / "dbt_rides" / "tests").glob("*.sql")}
    ran: set[str] = set()
    for args in STEPS.values():
        ran |= _tests_run_by(args)
    dead = sorted(files - ran)
    assert not dead, (
        f"тести {dead} не виконує жоден крок DAG-у (silver / gold / reconcile). "
        "Імовірно, вони посилаються на моделі різних шарів: `cautious` їх пропускає. "
        "Додайте на початку файлу {{ config(tags=['reconcile']) }}."
    )
