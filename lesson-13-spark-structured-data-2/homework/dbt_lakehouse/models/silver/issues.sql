-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».
-- Джерело: {{ ref('events') }}, типи IssuesEvent ТА IssueCommentEvent (обидва несуть issue{}).
-- from_json(payload, ISSUE_SCHEMA), ISSUE_SCHEMA = var('issue_schema').
-- Грануляція: один рядок на (repo_name, issue_number) — стан з останньої за часом події.
-- Колонки: repo_name, issue_number, title, author_login, state, opened_at, closed_at,
--          comments, label_names, comment_events_seen, last_event_at, hours_to_close

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)        as repo_name,
    cast(null as int)           as issue_number,
    cast(null as string)        as title,
    cast(null as string)        as author_login,
    cast(null as string)        as state,
    cast(null as timestamp)     as opened_at,
    cast(null as timestamp)     as closed_at,
    cast(null as int)           as comments,
    cast(null as array<string>) as label_names,
    cast(null as bigint)        as comment_events_seen,
    cast(null as timestamp)     as last_event_at,
    cast(null as double)        as hours_to_close
where false
