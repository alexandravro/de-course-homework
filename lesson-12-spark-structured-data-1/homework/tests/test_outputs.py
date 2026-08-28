"""Приймальні перевірки артефактів у data/output/.

Числа детерміновані: джерело — фіксовані landing-файли у data/landing/.
Спочатку запустіть job: uv run python job.py
"""

from __future__ import annotations

import glob
import os

import polars as pl

EVENTS_TOTAL = 29_750
OWNERS_TOTAL = 14_822
SUMMARY_TOTAL = 25_102
TOP_N = 5
TARGET_TYPES = {
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
}
DIMENSIONS = {"event_type", "repo_owner", "actor_login", "hour"}


# ── events ────────────────────────────────────────────────────────────────────
def test_events_row_count(out_events: pl.DataFrame) -> None:
    assert out_events.height == EVENTS_TOTAL


def test_events_have_derived_columns(out_events: pl.DataFrame) -> None:
    for col in ("event_id", "actor_login", "repo_name", "repo_owner", "is_bot", "hour"):
        assert col in out_events.columns
    assert out_events["created_at"].dtype == pl.Datetime
    assert out_events["is_bot"].dtype == pl.Boolean


def test_events_deduplicated_by_id(out_events: pl.DataFrame) -> None:
    assert out_events["event_id"].n_unique() == out_events.height


def test_events_partitioned_by_type() -> None:
    parts = {os.path.basename(p) for p in glob.glob("data/output/events/event_type=*")}
    assert parts == {f"event_type={t}" for t in TARGET_TYPES}


def test_events_repo_owner_is_prefix_of_repo_name(out_events: pl.DataFrame) -> None:
    mismatched = out_events.filter(
        pl.col("repo_name").str.split("/").list.first() != pl.col("repo_owner")
    )
    assert mismatched.height == 0


# ── owner_totals ──────────────────────────────────────────────────────────────
def test_owner_totals_row_count(out_owner_totals: pl.DataFrame) -> None:
    assert out_owner_totals.height == OWNERS_TOTAL


def test_owner_totals_cover_every_event(out_owner_totals: pl.DataFrame) -> None:
    assert out_owner_totals["owner_events"].sum() == EVENTS_TOTAL


def test_owner_bot_events_never_exceed_owner_events(
    out_owner_totals: pl.DataFrame,
) -> None:
    assert out_owner_totals.filter(
        pl.col("owner_bot_events") > pl.col("owner_events")
    ).height == 0


# ── top_repos ─────────────────────────────────────────────────────────────────
def test_top_repos_five_per_type(out_top_repos: pl.DataFrame) -> None:
    assert out_top_repos.height == len(TARGET_TYPES) * TOP_N
    assert out_top_repos.group_by("event_type").len()["len"].max() <= TOP_N


def test_top_repos_ranks_are_one_to_n(out_top_repos: pl.DataFrame) -> None:
    per_type = out_top_repos.group_by("event_type").agg(
        pl.col("rank").sort().alias("ranks")
    )
    assert all(r == list(range(1, TOP_N + 1)) for r in per_type["ranks"].to_list())


def test_top_repos_enriched_with_owner_totals(out_top_repos: pl.DataFrame) -> None:
    for col in ("repo_owner", "owner_events", "owner_repos", "owner_share"):
        assert col in out_top_repos.columns
    assert out_top_repos["owner_share"].null_count() == 0
    assert 0 < out_top_repos["owner_share"].min()
    assert out_top_repos["owner_share"].max() <= 1


def test_top_repos_count_never_exceeds_owner_total(out_top_repos: pl.DataFrame) -> None:
    assert out_top_repos.filter(
        pl.col("repo_event_count") > pl.col("owner_events")
    ).height == 0


# ── summary ───────────────────────────────────────────────────────────────────
def test_summary_row_count(out_summary: pl.DataFrame) -> None:
    assert out_summary.height == SUMMARY_TOTAL


def test_summary_contains_every_dimension(out_summary: pl.DataFrame) -> None:
    assert set(out_summary["dimension"].unique().to_list()) == DIMENSIONS


def test_summary_each_dimension_covers_all_events(out_summary: pl.DataFrame) -> None:
    per_dim = out_summary.group_by("dimension").agg(pl.col("events").sum())
    assert set(per_dim["events"].to_list()) == {EVENTS_TOTAL}


def test_summary_grain_is_unique(out_summary: pl.DataFrame) -> None:
    assert out_summary.select("dimension", "dimension_value").n_unique() == out_summary.height


def test_summary_repo_owner_slice_matches_owner_totals(
    out_summary: pl.DataFrame, out_owner_totals: pl.DataFrame
) -> None:
    """Той самий grain, порахований двома різними шляхами."""
    owners = out_summary.filter(pl.col("dimension") == "repo_owner")
    assert owners.height == out_owner_totals.height
    assert owners["events"].sum() == out_owner_totals["owner_events"].sum()


# ── запис (крок 10) ───────────────────────────────────────────────────────────
def test_unpartitioned_marts_are_single_files() -> None:
    for name in ("owner_totals", "top_repos", "summary"):
        files = glob.glob(f"data/output/{name}/*.parquet")
        assert len(files) == 1, f"{name}: очікували один файл, отримали {len(files)}"
