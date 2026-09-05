-- Крок 5: gold.dim_repo. Специфікація: ../../SPEC.md → «Крок 5».
-- Джерело: {{ ref('events') }}. Грануляція: один рядок на репозиторій.
-- Колонки: repo_id (md5(repo_name)), repo_name, repo_owner, first_seen_at, last_seen_at,
--          event_count, is_forked (є хоч одна подія ForkEvent по цьому репо).

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)    as repo_id,
    cast(null as string)    as repo_name,
    cast(null as string)    as repo_owner,
    cast(null as timestamp) as first_seen_at,
    cast(null as timestamp) as last_seen_at,
    cast(null as bigint)    as event_count,
    cast(null as boolean)   as is_forked
where false
