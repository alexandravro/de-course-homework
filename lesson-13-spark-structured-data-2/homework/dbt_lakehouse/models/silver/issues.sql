-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».
-- Джерело: {{ ref('events') }}, типи IssuesEvent ТА IssueCommentEvent (обидва несуть issue{}).
-- from_json(payload, ISSUE_SCHEMA), ISSUE_SCHEMA = var('issue_schema').
-- Грануляція: один рядок на (repo_name, issue_number) — стан з останньої за часом події.
-- Колонки: repo_name, issue_number, title, author_login, state, opened_at, closed_at,
--          comments, label_names, comment_events_seen, last_event_at, hours_to_close

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with parsed as (
    select
        event_id,
        event_type,
        repo_name,
        created_at                                          as event_at,
        from_json(payload, '{{ var("issue_schema") }}')     as p
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

ranked as (
    select
        repo_name,
        p.issue.number                                      as issue_number,
        p.issue.title                                       as title,
        p.issue.user.login                                  as author_login,
        p.issue.state                                       as state,
        to_timestamp(p.issue.created_at)                    as opened_at,
        to_timestamp(p.issue.closed_at)                     as closed_at,
        p.issue.comments                                    as comments,
        p.issue.labels.name                                 as label_names,
        event_at                                            as last_event_at,
        event_type,
        event_id,
        row_number() over (
            partition by repo_name, p.issue.number
            order by event_at desc, event_id desc
        )                                                   as rn,
        sum(case when event_type = 'IssueCommentEvent' then 1 else 0 end) over (
            partition by repo_name, p.issue.number
        )                                                   as comment_events_seen
    from parsed
    where p.issue.number is not null
)

select
    repo_name,
    issue_number,
    title,
    author_login,
    state,
    opened_at,
    closed_at,
    comments,
    label_names,
    comment_events_seen,
    last_event_at,
    case
        when closed_at is not null
        then (unix_timestamp(closed_at) - unix_timestamp(opened_at)) / 3600.0
        else null
    end                                                     as hours_to_close
from ranked
where rn = 1
