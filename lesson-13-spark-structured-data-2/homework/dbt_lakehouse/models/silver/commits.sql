-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)    as commit_sha,
    cast(null as string)    as repo_name,
    cast(null as string)    as pushed_by,
    cast(null as string)    as branch,
    cast(null as string)    as author_name,
    cast(null as string)    as author_email,
    cast(null as string)    as message,
    cast(null as boolean)   as is_distinct,
    cast(null as timestamp) as pushed_at,
    cast(null as boolean)   as is_merge_commit,
    cast(null as string)    as message_subject,
    cast(null as int)       as message_length
where false
