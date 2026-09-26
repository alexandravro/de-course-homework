"""Вантажить фікстурні батчі у bronze.raw_events БЕЗ Spark (psycopg COPY). ДАНО.

    uv run python -m scripts.load_bronze_fixture 1 2     # батчі 1 і 2
    uv run python -m scripts.load_bronze_fixture --all

Потрібен, щоб працювати над dbt-моделями, не чекаючи на власний consumer і Spark-job.
Поводиться як bronze_job: один `_ingested_at` на батч, файл, що вже завантажено, пропускає.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from common import config

FIXTURES = Path("data/fixtures")
COLUMNS = "event_id, event_type, ride_id, occurred_at, source, payload, _source_file, _ingested_at"


def load_batch(conn: psycopg.Connection, batch: int) -> int:
    root = FIXTURES / f"batch_{batch}"
    if not root.exists():
        raise SystemExit(f"немає {root} — запустіть: uv run python -m scripts.make_fixtures")
    loaded = {r[0] for r in conn.execute("select distinct _source_file from bronze.raw_events")}
    ingested_at = datetime.now(UTC)
    n_rows = 0
    with (
        conn.transaction(),
        conn.cursor() as cur,
        cur.copy(f"COPY bronze.raw_events ({COLUMNS}) FROM STDIN") as copy,
    ):
        for path in sorted(root.rglob("*.ndjson")):
            source_file = path.relative_to(root).as_posix()
            if source_file in loaded:
                continue
            for line in path.read_text().splitlines():
                event = json.loads(line)
                copy.write_row(
                    (
                        event["event_id"],
                        event["event_type"],
                        event["ride_id"],
                        event["occurred_at"],
                        event["source"],
                        json.dumps(event["payload"], separators=(",", ":")),
                        source_file,
                        ingested_at,
                    )
                )
                n_rows += 1
    return n_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batches", nargs="*", type=int)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    batches = sorted(p.name for p in FIXTURES.glob("batch_*")) if args.all else args.batches
    with psycopg.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=config.PG_DATABASE,
    ) as conn:
        for batch in batches:
            number = int(str(batch).removeprefix("batch_"))
            print(f"batch_{number}: додано {load_batch(conn, number)} рядків у bronze.raw_events")
            time.sleep(0.05)  # _ingested_at наступного батча гарантовано більший


if __name__ == "__main__":
    main()
