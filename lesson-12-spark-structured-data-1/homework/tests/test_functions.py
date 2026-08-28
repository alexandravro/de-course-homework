"""Unit-тести функцій job.py на крихітних DataFrame у пам'яті.

Швидкі — запускати Spark-job не треба. Кожен тест перевіряє рівно одну функцію
на фікстурі з conftest.py; контракти описані у SPEC.md.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

import job
from conftest import LANDING_SCHEMA


def _rows(df, *cols, order_by=None):
    """Зібрати DataFrame у список кортежів — зручно порівнювати в assert."""
    picked = df.select(*cols)
    if order_by:
        picked = picked.orderBy(*order_by)
    return [tuple(r) for r in picked.collect()]


# ── Крок 1 — event_schema ─────────────────────────────────────────────────────
def test_event_schema_matches_contract():
    assert job.event_schema() == LANDING_SCHEMA


# ── Крок 2 — flatten ──────────────────────────────────────────────────────────
def test_flatten_produces_flat_columns(raw):
    out = job.flatten(raw)
    assert out.columns == [
        "event_id",
        "event_type",
        "actor_login",
        "repo_name",
        "public",
        "created_at",
    ]


def test_flatten_parses_created_at_as_timestamp(raw):
    assert isinstance(job.flatten(raw).schema["created_at"].dataType, TimestampType)


def test_flatten_does_not_filter(raw):
    assert job.flatten(raw).count() == raw.count()


def test_flatten_unnests_structs(raw):
    out = job.flatten(raw).filter(F.col("event_id") == "3")
    assert _rows(out, "actor_login", "repo_name") == [("ci[bot]", "globex/lib")]


def test_flatten_keeps_null_struct_as_null(raw):
    out = job.flatten(raw).filter(F.col("event_id") == "5")
    assert _rows(out, "repo_name") == [(None,)]


# ── Крок 3 — clean ────────────────────────────────────────────────────────────
def test_clean_keeps_only_target_event_types(flat):
    types = {r[0] for r in _rows(job.clean(flat), "event_type")}
    assert types <= set(job.TARGET_EVENT_TYPES)
    assert "ForkEvent" not in types


def test_clean_drops_rows_without_repo(flat):
    ids = {r[0] for r in _rows(job.clean(flat), "event_id")}
    assert "5" not in ids


def test_clean_drops_rows_with_null_public(flat):
    """public IS NULL — не те саме, що public = false. Рядок має зникнути."""
    ids = {r[0] for r in _rows(job.clean(flat), "event_id")}
    assert "6" not in ids


def test_clean_deduplicates_by_event_id(flat):
    out = job.clean(flat)
    assert out.count() == out.select("event_id").distinct().count()


def test_clean_result_is_exactly_expected(flat):
    assert sorted(r[0] for r in _rows(job.clean(flat), "event_id")) == ["1", "2", "3", "7"]


# ── Крок 4 — with_derived ─────────────────────────────────────────────────────
def test_with_derived_adds_columns(flat):
    out = job.with_derived(job.clean(flat))
    for col in ("repo_owner", "is_bot", "hour"):
        assert col in out.columns


def test_with_derived_repo_owner_is_prefix_before_slash(flat):
    out = job.with_derived(job.clean(flat))
    assert _rows(out, "event_id", "repo_owner", order_by=["event_id"]) == [
        ("1", "acme"),
        ("2", "acme"),
        ("3", "globex"),
        ("7", "globex"),
    ]


def test_with_derived_is_bot_detects_bot_suffix(flat):
    out = job.with_derived(job.clean(flat))
    assert _rows(out, "event_id", "is_bot", order_by=["event_id"]) == [
        ("1", False),
        ("2", False),
        ("3", True),
        ("7", False),  # actor_login IS NULL → False, а не NULL
    ]


def test_with_derived_hour_truncates_to_hour(flat):
    out = job.with_derived(job.clean(flat))
    assert _rows(out, "event_id", "hour", order_by=["event_id"]) == [
        ("1", datetime(2024, 1, 15, 12)),
        ("2", datetime(2024, 1, 15, 12)),
        ("3", datetime(2024, 1, 15, 13)),
        ("7", datetime(2024, 1, 15, 14)),
    ]


# ── Крок 5 — owner_totals ─────────────────────────────────────────────────────
def test_owner_totals_one_row_per_owner(events):
    out = job.owner_totals(events)
    assert _rows(
        out,
        "repo_owner",
        "owner_events",
        "owner_repos",
        "owner_bot_events",
        order_by=["repo_owner"],
    ) == [
        ("acme", 2, 2, 0),
        ("globex", 2, 2, 1),
    ]


# ── Крок 6 — top_repos_per_type ───────────────────────────────────────────────
def test_top_repos_columns(events):
    out = job.top_repos_per_type(events, 5)
    assert out.columns == ["event_type", "repo_name", "repo_event_count", "rank"]


def test_top_repos_ranks_within_each_type(events):
    out = job.top_repos_per_type(events, 5)
    assert _rows(
        out, "event_type", "repo_name", "rank", order_by=["event_type", "rank"]
    ) == [
        ("IssuesEvent", "globex/lib", 1),
        # нічия за кількістю → тай-брейк за repo_name за зростанням
        ("PushEvent", "acme/api", 1),
        ("PushEvent", "globex/cli", 2),
        ("WatchEvent", "acme/web", 1),
    ]


def test_top_repos_respects_n(events):
    out = job.top_repos_per_type(events, 1)
    assert out.count() == 3  # по одному репозиторію на кожен із трьох типів
    assert {r[0] for r in _rows(out, "rank")} == {1}


# ── Крок 7 — enrich_top_repos ─────────────────────────────────────────────────
def test_enrich_top_repos_attaches_owner_totals(events):
    top = job.top_repos_per_type(events, 5)
    out = job.enrich_top_repos(top, job.owner_totals(events))
    assert _rows(
        out, "repo_name", "repo_owner", "owner_events", "owner_share", order_by=["repo_name"]
    ) == [
        ("acme/api", "acme", 2, 0.5),
        ("acme/web", "acme", 2, 0.5),
        ("globex/cli", "globex", 2, 0.5),
        ("globex/lib", "globex", 2, 0.5),
    ]


def test_enrich_top_repos_survives_missing_owner(events, spark):
    """LEFT JOIN без збігу: лічильники → 0, частка → NULL (а не помилка ділення)."""
    top = job.top_repos_per_type(events, 5)
    only_acme = job.owner_totals(events).filter(F.col("repo_owner") == "acme")
    out = job.enrich_top_repos(top, only_acme).filter(F.col("repo_owner") == "globex")
    assert _rows(out, "owner_events", "owner_repos", "owner_share") == [
        (0, 0, None),
        (0, 0, None),
    ]


# ── Крок 8 — summary_slice ────────────────────────────────────────────────────
def test_summary_slice_columns(events):
    out = job.summary_slice(events, "event_type")
    assert out.columns == ["dimension", "dimension_value", "events", "distinct_repos"]


def test_summary_slice_aggregates_by_given_dimension(events):
    out = job.summary_slice(events, "event_type")
    assert _rows(
        out, "dimension", "dimension_value", "events", "distinct_repos",
        order_by=["dimension_value"],
    ) == [
        ("event_type", "IssuesEvent", 1, 1),
        ("event_type", "PushEvent", 2, 2),
        ("event_type", "WatchEvent", 1, 1),
    ]


def test_summary_slice_uses_the_argument_not_a_hardcoded_column(events):
    """Той самий код має працювати для будь-якого виміру зі списку."""
    out = job.summary_slice(events, "repo_owner")
    assert _rows(
        out, "dimension", "dimension_value", "events", order_by=["dimension_value"]
    ) == [
        ("repo_owner", "acme", 2),
        ("repo_owner", "globex", 2),
    ]


def test_summary_slice_casts_dimension_value_to_string(events):
    out = job.summary_slice(events, "hour")
    assert isinstance(out.schema["dimension_value"].dataType, StringType)


# ── Крок 9 — build_summary ────────────────────────────────────────────────────
def test_build_summary_stacks_every_dimension(events):
    out = job.build_summary(events, ["event_type", "repo_owner"])
    assert out.columns == ["dimension", "dimension_value", "events", "distinct_repos"]
    assert {r[0] for r in _rows(out, "dimension")} == {"event_type", "repo_owner"}
    assert out.count() == 5  # 3 типи + 2 власники


def test_build_summary_every_slice_covers_all_events(events):
    """Кожен вимір ріже той самий набір подій — суми мають збігатися."""
    out = job.build_summary(events, job.SUMMARY_DIMENSIONS)
    per_dim = _rows(
        out.groupBy("dimension").agg(F.sum("events").alias("total")),
        "dimension",
        "total",
        order_by=["dimension"],
    )
    assert {total for _, total in per_dim} == {events.count()}
    assert len(per_dim) == len(job.SUMMARY_DIMENSIONS)


# ── Крок 10 — write_outputs ───────────────────────────────────────────────────
def test_write_outputs_writes_single_file_per_mart(events, tmp_path, monkeypatch):
    monkeypatch.setattr(job, "OUTPUT_DIR", str(tmp_path))
    job.write_outputs({"owner_totals": (events, None)})
    written = list((tmp_path / "owner_totals").glob("*.parquet"))
    assert len(written) == 1


def test_write_outputs_partitions_when_column_given(events, tmp_path, monkeypatch):
    monkeypatch.setattr(job, "OUTPUT_DIR", str(tmp_path))
    job.write_outputs({"events": (events, "event_type")})
    parts = {p.name for p in (tmp_path / "events").glob("event_type=*")}
    assert parts == {
        "event_type=PushEvent",
        "event_type=WatchEvent",
        "event_type=IssuesEvent",
    }


def test_write_outputs_handles_every_entry(events, tmp_path, monkeypatch):
    monkeypatch.setattr(job, "OUTPUT_DIR", str(tmp_path))
    job.write_outputs({"a": (events, None), "b": (events, None), "c": (events, "repo_owner")})
    assert {d.name for d in tmp_path.iterdir()} == {"a", "b", "c"}
