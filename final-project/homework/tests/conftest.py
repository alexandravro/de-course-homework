"""Спільні фікстури й хелпери тестів. ДАНО. Не редагуйте."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "data" / "fixtures"
DBT_DIRS = ["--project-dir", str(ROOT / "dbt_rides"), "--profiles-dir", str(ROOT / "dbt_rides")]


def pg_params() -> dict[str, str | int]:
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5433")),
        "user": os.environ.get("PGUSER", "taxi"),
        "password": os.environ.get("PGPASSWORD", "taxi"),
        "dbname": os.environ.get("PGDATABASE", "taxi_dwh"),
    }


@pytest.fixture(scope="session")
def pg() -> Iterator[psycopg.Connection]:
    try:
        conn = psycopg.connect(**pg_params(), autocommit=True, connect_timeout=5)  # type: ignore[arg-type]
    except psycopg.OperationalError as exc:
        pytest.fail(f"Postgres недоступний ({exc}). Запустіть: docker compose up -d", pytrace=False)
    yield conn
    conn.close()


def copy_batches(landing: Path, *batches: int) -> None:
    """Копіює фікстурні батчі в landing так, ніби consumer щойно їх записав."""
    for batch in batches:
        shutil.copytree(FIXTURES / f"batch_{batch}", landing, dirs_exist_ok=True)


def count_lines(*batches: int) -> int:
    return sum(
        1
        for batch in batches
        for path in (FIXTURES / f"batch_{batch}").rglob("*.ndjson")
        for _ in path.open()
    )


def run_module(
    module: str, *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


def run_dbt(*args: str) -> subprocess.CompletedProcess[str]:
    dbt = Path(sys.executable).parent / "dbt"
    env = {
        **os.environ,
        "DBT_TARGET_PATH": "/tmp/fp-dbt-target",
        "DBT_LOG_PATH": "/tmp/fp-dbt-logs",
        # Незвичний часовий пояс сесії (UTC+13:45): модель, що неявно залежить від поясу клієнта,
        # тут дасть інші дати й межі годин, ніж очікує assert_dates_are_utc.
        "PGTZ": "Pacific/Chatham",
    }
    return subprocess.run(
        [str(dbt), *args, *DBT_DIRS],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


def require_implemented(stage: str) -> None:
    """Зупиняє pytest ДО запуску Docker / Spark / dbt, якщо етап ще не написаний."""
    from scripts.precheck import expand, unfinished

    problems = [f"{name}: {p}" for name in expand(stage) for p in unfinished(name)]
    if problems:
        pytest.exit(
            "рішення ще нема, заглушки не замінено (Docker не запускається):\n  - "
            + "\n  - ".join(problems),
            returncode=1,
        )
