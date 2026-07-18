"""Silver stage — clean, filter and de-duplicate the bronze events.

TODO (Завдання 2 і 3): реалізуйте build_silver() і write_silver_partitioned().
Контракт: див. CONTRACTS.md → "silver" і "silver partitioned".

build_silver():
  * залиште тільки типи з config.TARGET_EVENT_TYPES
  * приберіть рядки з порожнім/відсутнім repo_name, відсутнім event_id чи created_at
  * гарантуйте унікальність по event_id (.unique(subset=["event_id"]))
  * запишіть у config.SILVER_FILE і поверніть DataFrame

write_silver_partitioned():
  * запишіть silver як Hive-партиціонований датасет за event_type
  * директорія: config.SILVER_PARTITIONED_DIR
  * підказка: df.write_parquet(dir, partition_by="event_type")
"""

from __future__ import annotations

import os

import polars as pl

from . import config


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    os.makedirs(os.path.dirname(config.SILVER_FILE), exist_ok=True)

    df = (
        bronze
        .filter(pl.col("event_type").is_in(config.TARGET_EVENT_TYPES))
        .filter(pl.col("repo_name").is_not_null() & (pl.col("repo_name") != ""))
        .filter(pl.col("event_id").is_not_null())
        .filter(pl.col("created_at").is_not_null())
        .unique(subset=["event_id"])
    )

    df.write_parquet(config.SILVER_FILE)
    return df


def write_silver_partitioned(silver: pl.DataFrame) -> None:
    os.makedirs(config.SILVER_PARTITIONED_DIR, exist_ok=True)
    silver.write_parquet(config.SILVER_PARTITIONED_DIR, partition_by="event_type")
