"""Спільні хелпери перевірок складу: контрольні числа, стан таблиць, хеші. ДАНО."""

from __future__ import annotations

from typing import Any

import psycopg

# (рядків Bronze, silver.events, silver.rides, completed, cancelled, з paid_at) після батча N
AFTER_BATCH = {
    1: (1437, 1367, 393, 137, 57, 129),
    2: (2874, 2717, 695, 371, 92, 359),
    3: (4311, 4071, 987, 624, 131, 615),
    4: (5745, 5447, 1171, 1008, 163, 1008),
}

# таблиця -> (колонки сортування). Аудит-колонки (`_…`) у порівняння не входять.
TABLES = {
    "silver.events": "event_id",
    "silver.rides": "ride_id",
    "gold.dim_zone": "zone_key",
    "gold.dim_driver": "driver_key",
    "gold.fact_ride": "ride_id",
    "gold.agg_zone_hourly": "requested_hour, pickup_zone_key",
}

STEPS = (
    ["build", "--selector", "silver", "--indirect-selection", "cautious"],
    ["build", "--selector", "gold", "--indirect-selection", "cautious"],
    ["test", "--selector", "reconcile"],
)


def one(pg: psycopg.Connection, sql: str) -> Any:
    row = pg.execute(sql).fetchone()
    assert row is not None
    return row[0]


def state(pg: psycopg.Connection) -> tuple[int, ...]:
    return (
        one(pg, "select count(*) from bronze.raw_events"),
        one(pg, "select count(*) from silver.events"),
        one(pg, "select count(*) from silver.rides"),
        one(pg, "select count(*) from silver.rides where status = 'completed'"),
        one(pg, "select count(*) from silver.rides where status = 'cancelled'"),
        one(pg, "select count(*) from silver.rides where paid_at is not null"),
    )


def table_hashes(pg: psycopg.Connection) -> dict[str, str]:
    """md5 бізнес-колонок кожної таблиці. `_ingested_at` / `_loaded_at` / `_source_file` — ні."""
    hashes = {}
    for table, order in TABLES.items():
        schema, name = table.split(".")
        cols = [
            r[0]
            for r in pg.execute(
                "select column_name from information_schema.columns"
                " where table_schema = %s and table_name = %s and column_name not like '\\_%%'"
                " order by ordinal_position",
                (schema, name),
            ).fetchall()
        ]
        sql = (
            f"select md5(coalesce(string_agg(t::text, '|' order by {order}), '')) "
            f"from (select {', '.join(cols)} from {table}) t"
        )
        hashes[table] = one(pg, sql)
    return hashes
