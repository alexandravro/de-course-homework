"""
Перевірки виводу streaming-job (pytest на даних, не на коді).

Тести читають parquet/json, які згенерував `streaming_job.py`, і звіряють їх із
зафіксованими контрольними числами для семплу `2024-01-15-12` (перші 12 000 подій).
Запускати з каталогу homework/ ПІСЛЯ прогону job:
    cd homework && uv run python streaming_job.py && uv run pytest -q
"""

import json
import os

import polars as pl
import pytest

WINDOWED = "data/output/windowed/*.parquet"
SUMMARY = "data/output/summary.json"

KEEP_TYPES = {"PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent", "WatchEvent"}

# Контрольні числа (deterministic для зафіксованого семплу + UTC timezone)
EXPECTED_TOTAL = 9785
EXPECTED_N_WINDOWS = 6
EXPECTED_BY_TYPE = {
    "PushEvent": 7977,
    "PullRequestEvent": 783,
    "IssueCommentEvent": 490,
    "WatchEvent": 373,
    "IssuesEvent": 162,
}
EXPECTED_WINDOW_TOTALS = {
    "2024-01-15 12:00:00": 1748,
    "2024-01-15 12:00:30": 1755,
    "2024-01-15 12:01:00": 1727,
    "2024-01-15 12:01:30": 1746,
    "2024-01-15 12:02:00": 1750,
    "2024-01-15 12:02:30": 1059,
}
EXPECTED_FIRST_WINDOW_PUSH = 1468


@pytest.fixture(scope="module")
def windows() -> pl.DataFrame:
    if not pl.scan_parquet(WINDOWED).collect().height:
        pytest.fail("Порожній вивід — спершу прогоніть streaming_job.py")
    return pl.read_parquet(WINDOWED)


@pytest.fixture(scope="module")
def summary() -> dict:
    if not os.path.exists(SUMMARY):
        pytest.fail(f"Немає {SUMMARY} — спершу прогоніть streaming_job.py")
    with open(SUMMARY) as f:
        return json.load(f)


# --- Завдання 1–2: схема + чистка (правильні колонки і тільки 5 публічних типів) ---
def test_output_columns(windows: pl.DataFrame) -> None:
    assert set(windows.columns) == {"window_start", "window_end", "event_type", "event_count"}


def test_only_kept_types(windows: pl.DataFrame) -> None:
    assert set(windows["event_type"].unique().to_list()) <= KEEP_TYPES


# --- Завдання 3–4: tumbling window + watermark + запис ---
def test_distinct_windows(windows: pl.DataFrame) -> None:
    assert windows["window_start"].n_unique() == EXPECTED_N_WINDOWS


def test_window_totals(windows: pl.DataFrame) -> None:
    got = {
        str(r["window_start"]): r["total"]
        for r in windows.group_by("window_start")
        .agg(pl.col("event_count").sum().alias("total"))
        .iter_rows(named=True)
    }
    assert got == EXPECTED_WINDOW_TOTALS


def test_first_window_pushevent(windows: pl.DataFrame) -> None:
    first = windows["window_start"].min()
    val = windows.filter(
        (pl.col("window_start") == first) & (pl.col("event_type") == "PushEvent")
    )["event_count"].to_list()
    assert val == [EXPECTED_FIRST_WINDOW_PUSH]


def test_total_events(windows: pl.DataFrame) -> None:
    assert windows["event_count"].sum() == EXPECTED_TOTAL


# --- Завдання 5–6: serving summary ---
def test_summary_totals(summary: dict) -> None:
    assert summary["total_events"] == EXPECTED_TOTAL
    assert summary["n_windows"] == EXPECTED_N_WINDOWS


def test_summary_by_type(summary: dict) -> None:
    assert summary["by_type"] == EXPECTED_BY_TYPE
