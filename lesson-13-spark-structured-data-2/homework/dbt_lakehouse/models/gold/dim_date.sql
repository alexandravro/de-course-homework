-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».
-- Згенерований безперервний календар (БЕЗ seed): explode(sequence(min, max, interval 1 day)).
-- Межі min/max — підзапитом по фактичних датах з {{ ref('commits') }}, {{ ref('pull_requests') }},
-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Не хардкодьте.
-- Колонки: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as int)     as date_id,
    cast(null as date)    as date_day,
    cast(null as int)     as day_of_week,
    cast(null as boolean) as is_weekend,
    cast(null as int)     as iso_week,
    cast(null as int)     as year
where false
