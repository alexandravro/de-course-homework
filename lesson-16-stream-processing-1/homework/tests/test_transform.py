# Offline unit tests for the pure functions (transform + consumer aggregation).
# No Kafka and no network here — only synthetic records.
import consumer
import transform

PUSH_EVENT = {
    "id": "100",
    "type": "PushEvent",
    "actor": {"login": "octocat"},
    "repo": {"name": "octocat/Hello-World"},
    "payload": {"ref": "refs/heads/main", "commits": [{"sha": "a"}, {"sha": "b"}]},
    "public": True,
    "created_at": "2024-01-15T14:00:01Z",
}

ISSUES_EVENT = {
    "id": "101",
    "type": "IssuesEvent",
    "actor": {"login": "hubot"},
    "repo": {"name": "octocat/Spoon"},
    "payload": {"action": "opened"},
    "public": True,
    "created_at": "2024-01-15T14:00:02Z",
}


# ---- Task 1: flatten_event ----

def test_flatten_push_event():
    rec = transform.flatten_event(PUSH_EVENT)
    assert rec["id"] == "100"
    assert rec["event_type"] == "PushEvent"
    assert rec["actor_login"] == "octocat"
    assert rec["repo_name"] == "octocat/Hello-World"
    assert rec["public"] is True
    assert rec["payload_ref"] == "refs/heads/main"
    assert rec["payload_commit_count"] == 2
    assert rec["payload_action"] is None


def test_flatten_created_at_is_epoch_millis():
    rec = transform.flatten_event(PUSH_EVENT)
    # 2024-01-15T14:00:01Z == 1705327201 s == 1705327201000 ms
    assert rec["created_at"] == 1705327201000
    assert isinstance(rec["created_at"], int)


def test_flatten_nullable_payload_fields():
    rec = transform.flatten_event(ISSUES_EVENT)
    assert rec["payload_action"] == "opened"
    assert rec["payload_ref"] is None
    assert rec["payload_commit_count"] is None


# ---- Task 2: event_filter ----

def test_filter_keeps_allowed_public():
    assert transform.event_filter(PUSH_EVENT) is True
    assert transform.event_filter(ISSUES_EVENT) is True


def test_filter_drops_other_types():
    assert transform.event_filter({"type": "ForkEvent", "public": True}) is False
    assert transform.event_filter({"type": "DeleteEvent", "public": True}) is False


def test_filter_drops_private():
    assert transform.event_filter({"type": "PushEvent", "public": False}) is False


# ---- Task 4: update_counts ----

def test_update_counts_accumulates():
    by_type, by_repo = {}, {}
    events = [
        {"event_type": "PushEvent", "repo_name": "a/a"},
        {"event_type": "PushEvent", "repo_name": "a/a"},
        {"event_type": "WatchEvent", "repo_name": "b/b"},
    ]
    for e in events:
        consumer.update_counts(by_type, by_repo, e)
    assert by_type == {"PushEvent": 2, "WatchEvent": 1}
    assert by_repo == {"a/a": 2, "b/b": 1}


# ---- Task 5: top_repos ----

def test_top_repos_orders_and_limits():
    by_repo = {"a/a": 5, "b/b": 9, "c/c": 9, "d/d": 1}
    top = consumer.top_repos(by_repo, n=2)
    # busiest first; ties broken by name -> b/b before c/c
    assert top == [["b/b", 9], ["c/c", 9]]


def test_top_repos_shorter_than_n():
    assert consumer.top_repos({"a/a": 3}, n=5) == [["a/a", 3]]
