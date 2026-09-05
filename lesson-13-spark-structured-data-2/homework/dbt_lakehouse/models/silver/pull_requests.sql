-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».
-- Джерело: {{ ref('events') }}, лише PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').
-- Грануляція: один рядок на (repo_name, pr_number) — стан з ОСТАННЬОЇ за часом події (row_number desc).
-- Колонки: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,
--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,
--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)          as repo_name,
    cast(null as int)             as pr_number,
    cast(null as string)          as title,
    cast(null as string)          as author_login,
    cast(null as string)          as state,
    cast(null as boolean)         as is_merged,
    cast(null as boolean)         as is_draft,
    cast(null as timestamp)       as opened_at,
    cast(null as timestamp)       as closed_at,
    cast(null as timestamp)       as merged_at,
    cast(null as int)             as additions,
    cast(null as int)             as deletions,
    cast(null as int)             as changed_files,
    cast(null as int)             as commits_count,
    cast(null as int)             as comments,
    cast(null as int)             as review_comments,
    cast(null as string)          as author_association,
    cast(null as array<string>)   as label_names,
    cast(null as string)          as last_action,
    cast(null as timestamp)       as last_event_at,
    cast(null as int)             as churn,
    cast(null as double)          as hours_open
where false
