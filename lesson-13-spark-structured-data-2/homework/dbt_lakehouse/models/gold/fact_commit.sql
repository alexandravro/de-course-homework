-- Крок 8: gold.fact_commit. Специфікація: ../../SPEC.md → «Крок 8».
-- Джерело: {{ ref('commits') }}. Грануляція не змінюється (1 рядок = 1 commit_sha).
-- FK-колонки: repo_id = md5(repo_name), pusher_id = md5(pushed_by),
--             date_id = cast(date_format(pushed_at,'yyyyMMdd') as int) — той самий вираз, що й у вимірах.
-- Колонки: commit_sha, repo_id, pusher_id, date_id, branch, is_merge_commit, is_distinct, message_length.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)  as commit_sha,
    cast(null as string)  as repo_id,
    cast(null as string)  as pusher_id,
    cast(null as int)     as date_id,
    cast(null as string)  as branch,
    cast(null as boolean) as is_merge_commit,
    cast(null as boolean) as is_distinct,
    cast(null as int)     as message_length
where false
