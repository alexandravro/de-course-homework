# End-to-end checkpoint test (Tasks 3 + 6).
# Runs after verify.sh has executed the producer and the consumer; asserts on
# the aggregated data/output/stats.json. The numbers are fixed by the pinned
# gharchive hour (2024-01-15-14) and MAX_RAW=100_000 in producer.py/consumer.py.
import json
import os

import pytest

OUTPUT_PATH = "data/output/stats.json"

# Deterministic checkpoint for the first 100 000 raw records of the pinned hour.
EXPECTED_TOTAL = 79646
EXPECTED_BY_TYPE = {
    "PushEvent": 62871,
    "PullRequestEvent": 7136,
    "IssueCommentEvent": 4421,
    "WatchEvent": 3505,
    "IssuesEvent": 1713,
}
EXPECTED_TOP_REPO = ["LMAO-armv8/kernel_samsung_r0q", 1182]


@pytest.fixture(scope="module")
def stats():
    if not os.path.exists(OUTPUT_PATH):
        pytest.fail(f"{OUTPUT_PATH} not found — run the producer and consumer first.")
    with open(OUTPUT_PATH) as f:
        return json.load(f)


def test_total_count(stats):
    assert stats["total"] == EXPECTED_TOTAL


def test_by_type_matches_checkpoint(stats):
    assert stats["by_type"] == EXPECTED_BY_TYPE


def test_by_type_sums_to_total(stats):
    assert sum(stats["by_type"].values()) == stats["total"]


def test_top_repos_shape(stats):
    top = stats["top_repos"]
    assert len(top) == 5
    counts = [c for _, c in top]
    assert counts == sorted(counts, reverse=True)
    assert all(c > 0 for c in counts)


def test_top_repo_is_deterministic(stats):
    assert stats["top_repos"][0] == EXPECTED_TOP_REPO
