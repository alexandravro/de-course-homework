-- Крок 8: gold.fact_commit. Специфікація: ../../SPEC.md → «Крок 8».
-- Джерело: {{ ref('commits') }}. Грануляція не змінюється (1 рядок = 1 commit_sha).
-- FK-колонки: repo_id = md5(repo_name), pusher_id = md5(pushed_by),
--             date_id = cast(date_format(pushed_at,'yyyyMMdd') as int) — той самий вираз, що й у вимірах.
-- Колонки: commit_sha, repo_id, pusher_id, date_id, branch, is_merge_commit, is_distinct, message_length.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    commit_sha,
    md5(repo_name)                                          as repo_id,
    md5(pushed_by)                                          as pusher_id,
    cast(date_format(pushed_at, 'yyyyMMdd') as int)         as date_id,
    branch,
    is_merge_commit,
    is_distinct,
    message_length
from {{ ref('commits') }}

