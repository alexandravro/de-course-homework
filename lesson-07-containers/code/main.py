"""Демо-застосунок для уроку про контейнери.

Рахує події з events.csv і друкує підсумок. Якщо задано змінну PGHOST — додатково
пише підсумок у Postgres (це показуємо в частині про Docker Compose).
"""

import csv
import os
from collections import Counter
import psycopg

from tabulate import tabulate


def load_events(path="events.csv"):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def summarize(rows):
    counts = Counter(row["event_type"] for row in rows)
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)


def write_to_postgres(summary):

    conn = psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS event_counts (event_type TEXT PRIMARY KEY, n INT)"
        )
        cur.execute("TRUNCATE event_counts")
        cur.executemany(
            "INSERT INTO event_counts (event_type, n) VALUES (%s, %s)", summary
        )
    conn.close()


def main():
    rows = load_events()
    summary = summarize(rows)
    print(tabulate(summary, headers=["event_type", "count"]))
    print(f"\nусього подій: {len(rows)}")

    if os.environ.get("PGHOST"):
        write_to_postgres(summary)
        print(f"-> записано {len(summary)} рядків у Postgres ({os.environ['PGHOST']})")


if __name__ == "__main__":
    main()
