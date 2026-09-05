-- Крок 6: gold.dim_actor. Специфікація: ../../SPEC.md → «Крок 6».
-- Джерело: {{ ref('events') }}, actor_login is not null. Грануляція: один рядок на актора.
-- Колонки: actor_id (md5(actor_login)), actor_login, is_bot (закінчується на [bot]),
--          first_seen_at, last_seen_at, event_count, distinct_repos.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)    as actor_id,
    cast(null as string)    as actor_login,
    cast(null as boolean)   as is_bot,
    cast(null as timestamp) as first_seen_at,
    cast(null as timestamp) as last_seen_at,
    cast(null as bigint)    as event_count,
    cast(null as bigint)    as distinct_repos
where false
