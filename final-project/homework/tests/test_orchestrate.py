"""Airflow: структура DAG-у і запуски поспіль. Потрібен Docker (образ Airflow збирається сам).

Тест піднімає профіль `airflow`, перевіряє структуру DAG-у всередині контейнера, а потім
проганяє його трьома запусками `airflow dags test` на фікстурах:

  1. батчі 1–2 → стан збігається з таблицею SPEC.md (розділ 6.3);
  2. + батчі 3–4 → фінальні числа (розділ 6.2);
  3. без нових файлів → жодних змін, ані рядка.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from typing import Any

import psycopg
import pytest

from scripts import reset_warehouse
from tests.conftest import ROOT, copy_batches, pg_params, require_implemented
from tests.warehouse import AFTER_BATCH, state, table_hashes

pytestmark = pytest.mark.warehouse

DAG_ID = "rides_medallion"
COMPOSE = ["docker", "compose", "--profile", "airflow"]
LANDING_REL = (
    "data/verify/landing"  # у шляху має бути `/landing/`: від нього рахується _source_file
)


def compose_exec(
    *cmd: str, env: dict[str, str] | None = None, timeout: int = 1800
) -> subprocess.CompletedProcess[str]:
    args = [*COMPOSE, "exec", "-T"]
    for key, value in (env or {}).items():
        args += ["-e", f"{key}={value}"]
    return subprocess.run(
        [*args, "airflow-scheduler", *cmd],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _meta(sql: str) -> list[tuple[Any, ...]]:
    """Запит до метабази Airflow (окрема база `airflow` на тому самому Postgres)."""
    params = {**pg_params(), "user": "airflow", "password": "airflow", "dbname": "airflow"}
    with psycopg.connect(**params, autocommit=True) as conn:  # type: ignore[arg-type]
        return conn.execute(sql).fetchall()


def _quiesce_scheduler() -> None:
    """Scheduler не має запускати DAG паралельно з тестами: вони пишуть в ті самі таблиці.

    DAG міг бути ввімкнений раніше (`./up.sh --airflow` вмикає його і це зберігається в метабазі),
    тоді scheduler запускав би його кожні 5 хвилин по живому landing прямо під час перевірки.
    Тому ставимо DAG на паузу й чекаємо, поки завершаться вже запущені scheduler-ом ранів.
    """
    deadline = time.monotonic() + 600
    rows: list[tuple[Any, ...]] = []
    while time.monotonic() < deadline:
        try:
            rows = _meta(f"select is_paused from dag where dag_id = '{DAG_ID}'")
        except psycopg.Error:
            rows = []  # метабаза ще мігрується
        if rows:
            break
        time.sleep(3)
    else:
        pytest.fail("DAG не зареєструвався в метабазі Airflow за 10 хвилин")
    if not rows[0][0]:
        compose_exec("airflow", "dags", "pause", DAG_ID, timeout=120)
    while time.monotonic() < deadline:
        active = _meta(
            f"select count(*) from dag_run where dag_id = '{DAG_ID}' "
            "and state in ('running', 'queued') and run_id not like 'manual__2024-01-15T00:00:00%'"
        )[0][0]
        if active == 0:
            return
        time.sleep(5)
    pytest.fail("scheduler-ові рани DAG не завершились за 10 хвилин")


@pytest.fixture(scope="module", autouse=True)
def airflow_up() -> None:
    require_implemented("orchestrate")
    up = subprocess.run(
        [*COMPOSE, "up", "-d", "--build", "airflow-webserver", "airflow-scheduler"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    assert up.returncode == 0, f"не вдалося підняти Airflow:\n{up.stderr[-3000:]}"
    deadline = time.monotonic() + 240
    responsive_since: float | None = None
    while time.monotonic() < deadline:
        probe = compose_exec("airflow", "dags", "list", timeout=120)
        if probe.returncode == 0:
            if DAG_ID in probe.stdout:
                _quiesce_scheduler()
                return
            responsive_since = responsive_since or time.monotonic()
            if time.monotonic() - responsive_since > 45:
                # Airflow працює, а DAG не бачить: не чекаємо 4 хвилини, а показуємо причину.
                errors = compose_exec("airflow", "dags", "list-import-errors", timeout=120)
                pytest.fail(
                    f"Airflow працює, але DAG {DAG_ID} не знайдено. Помилки імпорту DAG-ів:\n"
                    f"{errors.stdout[-2500:] or '(немає)'}",
                    pytrace=False,
                )
        time.sleep(5)
    pytest.fail("Airflow не побачив DAG rides_medallion за 4 хвилини (імпорт-помилка?)")


def run_dag() -> subprocess.CompletedProcess[str]:
    return compose_exec(
        "airflow",
        "dags",
        "test",
        "rides_medallion",
        "2024-01-15",
        env={"LANDING_DIR": f"/opt/airflow/project/{LANDING_REL}"},
    )


def succeeded(result: subprocess.CompletedProcess[str]) -> bool:
    return result.returncode == 0 and "state=success" in result.stdout + result.stderr


@pytest.fixture(scope="module")
def flow() -> dict[str, Any]:
    reset_warehouse.main()
    landing = ROOT / LANDING_REL
    shutil.rmtree(landing.parent, ignore_errors=True)
    landing.mkdir(parents=True)
    out: dict[str, Any] = {}
    try:
        copy_batches(landing, 1, 2)
        out["run1"] = run_dag()
        with psycopg.connect(**pg_params(), autocommit=True) as pg:  # type: ignore[arg-type]
            out["state1"] = state(pg)

        copy_batches(landing, 3, 4)
        out["run2"] = run_dag()
        with psycopg.connect(**pg_params(), autocommit=True) as pg:  # type: ignore[arg-type]
            out["state2"], out["hash2"] = state(pg), table_hashes(pg)

        out["run3"] = run_dag()  # без нових файлів
        with psycopg.connect(**pg_params(), autocommit=True) as pg:  # type: ignore[arg-type]
            out["state3"], out["hash3"] = state(pg), table_hashes(pg)
    finally:
        shutil.rmtree(landing.parent, ignore_errors=True)
    return out


def test_dag_structure_matches_spec() -> None:
    result = compose_exec("python", "/opt/airflow/project/scripts/check_dag.py", timeout=300)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-1000:]


@pytest.mark.parametrize("run", ["run1", "run2", "run3"])
def test_every_dag_run_succeeds(flow: dict[str, Any], run: str) -> None:
    result = flow[run]
    assert succeeded(result), (result.stdout + result.stderr)[-4000:]


def test_first_run_processes_first_two_batches(flow: dict[str, Any]) -> None:
    assert flow["state1"] == AFTER_BATCH[2]


def test_second_run_picks_up_only_new_files(flow: dict[str, Any]) -> None:
    assert flow["state2"] == AFTER_BATCH[4]


def test_run_without_new_files_changes_nothing(flow: dict[str, Any]) -> None:
    assert flow["state3"] == AFTER_BATCH[4]
    assert flow["hash3"] == flow["hash2"]
