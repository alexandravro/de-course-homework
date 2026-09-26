"""dbt Silver/Gold: інкремент батч за батчем, ідемпотентність, incremental == full refresh.

Потрібен `docker compose up -d`. Склад скидається; дані беруться з фікстур, тож Spark і Kafka не
потрібні — етап 2 не залежить від етапу 1.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest

from scripts import reset_warehouse
from scripts.load_bronze_fixture import load_batch
from tests.conftest import ROOT, pg_params, require_implemented, run_dbt
from tests.warehouse import AFTER_BATCH, STEPS, TABLES, one, state, table_hashes

pytestmark = pytest.mark.warehouse


def build_all(*extra: str) -> None:
    for step in STEPS:
        args = list(step)
        if extra and step[0] == "build":
            args += list(extra)
        result = run_dbt(*args)
        assert result.returncode == 0, f"dbt {' '.join(args)} впав:\n{result.stdout[-4000:]}"


def rides_snapshot(pg: psycopg.Connection) -> dict[str, tuple[Any, ...]]:
    rows = pg.execute(
        "select ride_id, status, paid_at is not null, pickup_zone_id from silver.rides"
    ).fetchall()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


@pytest.fixture(scope="module")
def run() -> dict[str, Any]:
    require_implemented("transform")
    reset_warehouse.main()
    out: dict[str, Any] = {"after": {}, "rides": {}}
    with psycopg.connect(**pg_params(), autocommit=True) as pg:  # type: ignore[arg-type]
        for batch in (1, 2, 3, 4):
            with psycopg.connect(**pg_params()) as conn:  # type: ignore[arg-type]
                load_batch(conn, batch)
            build_all()
            out["after"][batch] = state(pg)
            out["rides"][batch] = rides_snapshot(pg)

        out["final"] = final_numbers(pg)
        out["hash_incremental"] = table_hashes(pg)

        build_all()  # повторний запуск без нових даних
        out["hash_rerun"] = table_hashes(pg)
        out["state_rerun"] = state(pg)

        build_all("--full-refresh")
        out["hash_full_refresh"] = table_hashes(pg)
    return out


def final_numbers(pg: psycopg.Connection) -> dict[str, Any]:
    return {
        "dim_zone": one(pg, "select count(*) from gold.dim_zone"),
        "dim_driver": one(pg, "select count(*) from gold.dim_driver"),
        "fact_ride": one(pg, "select count(*) from gold.fact_ride"),
        "agg_rows": one(pg, "select count(*) from gold.agg_zone_hourly"),
        "revenue": one(pg, "select sum(total_amount) from silver.rides where status = 'completed'"),
        "tips": one(pg, "select sum(tip_amount) from silver.rides where status = 'completed'"),
        "null_tips": one(
            pg,
            "select count(*) from silver.rides where status = 'completed' and tip_amount is null",
        ),
        "pickup_moved": one(
            pg,
            "select count(*) from silver.rides r join silver.events e on e.ride_id = r.ride_id"
            " and e.event_type = 'ride_requested'"
            " where r.pickup_zone_id <> (e.payload #>> '{pickup,zone_id}')::int",
        ),
        "dropoff_moved": one(
            pg,
            "select count(*) from silver.rides r join silver.events e on e.ride_id = r.ride_id"
            " and e.event_type = 'ride_requested'"
            " where r.dropoff_zone_id <> (e.payload #>> '{dropoff,zone_id}')::int",
        ),
        "agg_requested": one(pg, "select sum(rides_requested) from gold.agg_zone_hourly"),
        "agg_revenue": one(pg, "select sum(gross_revenue) from gold.agg_zone_hourly"),
        "top_zones": [
            (r[0], r[1])
            for r in pg.execute(
                "select pickup_zone_key, sum(gross_revenue) from gold.agg_zone_hourly"
                " group by 1 order by 2 desc, 1 limit 3"
            ).fetchall()
        ],
    }


def test_state_after_each_batch_matches_checkpoints(run: dict[str, Any]) -> None:
    for batch, expected in AFTER_BATCH.items():
        assert run["after"][batch] == expected, f"після батча {batch}"


def test_final_numbers(run: dict[str, Any]) -> None:
    f = run["final"]
    assert (f["dim_zone"], f["dim_driver"], f["fact_ride"], f["agg_rows"]) == (266, 81, 1171, 351)
    assert f["revenue"] == Decimal("48972.86")
    assert f["tips"] == Decimal("4585.09")
    assert f["null_tips"] == 217
    assert (f["pickup_moved"], f["dropoff_moved"]) == (62, 39)
    assert f["agg_requested"] == 1171
    assert f["agg_revenue"] == Decimal("48972.86")
    assert f["top_zones"] == [
        (237, Decimal("2522.77")),
        (161, Decimal("2505.16")),
        (186, Decimal("2465.43")),
    ]


def test_late_events_update_rides_that_were_built_earlier(run: dict[str, Any]) -> None:
    """Фікстури справді містять запізнілі події, і інкремент їх підхопив, а не проігнорував."""
    early, final = run["rides"][1], run["rides"][4]
    late_paid = [r for r, s in early.items() if s[0] == "completed" and not s[1] and final[r][1]]
    assert late_paid, "у фікстурах нема поїздки, де payment_captured прийшов пізніше"
    moved = [r for r in early if r in final and early[r][2] != final[r][2]]
    assert moved, "у фікстурах нема поїздки, що змінила зону посадки між батчами"


def test_rerun_without_new_data_changes_nothing(run: dict[str, Any]) -> None:
    assert run["state_rerun"] == AFTER_BATCH[4]
    assert run["hash_rerun"] == run["hash_incremental"]


def test_incremental_equals_full_refresh(run: dict[str, Any]) -> None:
    diff = [t for t in TABLES if run["hash_incremental"][t] != run["hash_full_refresh"][t]]
    assert not diff, f"інкремент і --full-refresh розійшлися в: {diff}"


def test_students_added_at_least_two_own_dbt_tests() -> None:
    n_tests = len(list((ROOT / "dbt_rides" / "tests").glob("*.sql")))
    assert n_tests >= 13, f"у dbt_rides/tests/ {n_tests} тестів; 11 дано + щонайменше 2 ваші"
    assert Path(ROOT / "NOTES.md").exists()
