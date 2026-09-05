-- Тест: немає PR, де merged_at < opened_at або closed_at < opened_at.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
-- TODO: замініть заглушку (зараз тест проходить вхолосту).
select *
from {{ ref('pull_requests') }}
where false
