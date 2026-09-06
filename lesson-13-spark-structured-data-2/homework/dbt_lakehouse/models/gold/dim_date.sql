-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».
-- Згенерований безперервний календар (БЕЗ seed): explode(sequence(min, max, interval 1 day)).
-- Межі min/max — підзапитом по фактичних датах з {{ ref('commits') }}, {{ ref('pull_requests') }},
-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Не хардкодьте.
-- Колонки: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with date_bounds as (
    select date(pushed_at) as d from {{ ref('commits') }}
    union all
    select date(opened_at) from {{ ref('pull_requests') }}
    union all
    select date(merged_at) from {{ ref('pull_requests') }} where merged_at is not null
    union all
    select date(opened_at) from {{ ref('issues') }}
    union all
    select date(closed_at) from {{ ref('issues') }} where closed_at is not null
),

bounds as (
    select min(d) as min_date, max(d) as max_date
    from date_bounds
),

calendar as (
    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from bounds
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int)          as date_id,
    date_day,
    dayofweek(date_day)                                     as day_of_week,
    dayofweek(date_day) in (1, 7)                           as is_weekend,
    weekofyear(date_day)                                    as iso_week,
    year(date_day)                                          as year
from calendar

