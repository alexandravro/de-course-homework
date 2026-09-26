"""Захист корпусу подій: checkpoint-числа зі SPEC.md чинні, лише поки генератор незмінний."""

from __future__ import annotations

from stream.events import plan_corpus


def test_corpus_is_deterministic_and_matches_spec() -> None:
    planned = plan_corpus(42, 1200)
    ids = [p.event["event_id"] for p in planned]
    assert len(planned) == 5745
    assert len(set(ids)) == 5587
    assert sum(p.event["source"] == "loadtest" for p in planned) == 145
    assert [p.event["event_id"] for p in plan_corpus(42, 1200)] == ids
