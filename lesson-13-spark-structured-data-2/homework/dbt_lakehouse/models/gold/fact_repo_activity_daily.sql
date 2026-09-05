-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».
-- Грануляція: (repo_id, date_id). Багатоджерельний rollup з {{ ref('commits') }},
-- {{ ref('pull_requests') }}, {{ ref('issues') }} та {{ ref('events') }} (WatchEvent/ForkEvent).
-- Патерн: денний агрегат на джерело (метрика + нулі для решти) → union all → group by.
-- Відсутні метрики → 0, не NULL. Порядок і типи колонок у всіх CTE мають збігатися.
-- Колонки: activity_id (md5(concat_ws('|', repo_id, date_id))), repo_id, date_id, commits,
--          distinct_committers, prs_opened, prs_merged, issues_opened, issues_closed, stars, forks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string) as activity_id,
    cast(null as string) as repo_id,
    cast(null as int)    as date_id,
    cast(null as bigint) as commits,
    cast(null as bigint) as distinct_committers,
    cast(null as bigint) as prs_opened,
    cast(null as bigint) as prs_merged,
    cast(null as bigint) as issues_opened,
    cast(null as bigint) as issues_closed,
    cast(null as bigint) as stars,
    cast(null as bigint) as forks
where false
