# Завдання 1-2: чиста логіка перетворення (без Kafka, без мережі).
# Реалізуйте обидві функції. Їх перевіряє tests/test_transform.py.
from datetime import datetime

# Дано, не редагувати. П'ять типів подій, які лишаємо; решта відкидається.
ALLOWED_TYPES = (
    "PushEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "WatchEvent",
    "IssueCommentEvent",
)


def event_filter(event: dict) -> bool:
    return (
        event.get("public", True) is True
        and event.get("type") in ALLOWED_TYPES
    )    
    raise NotImplementedError("Реалізуйте event_filter")


def flatten_event(event: dict) -> dict:
    payload = event.get("payload", {})
    commits = payload.get("commits")
    return {
        "id": event["id"],
        "event_type": event["type"],
        "created_at": _to_millis(event["created_at"]),
        "actor_login": event["actor"]["login"],
        "repo_name": event["repo"]["name"],
        "public": event.get("public", True),
        "payload_action": payload.get("action"),
        "payload_ref": payload.get("ref"),
        "payload_commit_count": len(commits) if commits is not None else None,
    }
    raise NotImplementedError("Реалізуйте flatten_event")


def _to_millis(created_at: str) -> int:
    """ISO-8601 ('2024-01-15T14:00:01Z') -> epoch-мілісекунди (int). Готова функція."""
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)

