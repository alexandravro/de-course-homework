-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
-- TODO: замініть заглушку (зараз тест проходить вхолосту).
select 1
where (
    select sum(commits) from {{ ref('fact_repo_activity_daily') }}
) != (
    select count(*) from {{ ref('fact_commit') }}
)
