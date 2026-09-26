"""Скидає склад до чистого стану: порожній Bronze, без Silver / Gold / reference. ДАНО."""

from __future__ import annotations

import psycopg

from common import config


def main() -> None:
    with psycopg.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=config.PG_DATABASE,
        autocommit=True,
    ) as conn:
        conn.execute("TRUNCATE bronze.raw_events")
        conn.execute("DROP SCHEMA IF EXISTS silver, gold, reference CASCADE")
    print("склад скинуто: bronze порожній, silver / gold / reference видалено")


if __name__ == "__main__":
    main()
