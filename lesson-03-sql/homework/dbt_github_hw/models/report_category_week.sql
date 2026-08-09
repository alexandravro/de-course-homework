-- =====================================================================
-- TASK 7 — report_category_week (20 балів). Специфікація: ../../MODELS.md → «report_category_week».
--
-- Поряд лежить report_category_week_naive.sql — він НАВМИСНО неоптимізований:
-- join до calendar по strftime(event_date) = strftime(day) перетворює ключ join,
-- через що DuckDB сканує всі 14 партицій (немає ні propagation, ні partition pruning).
--
-- Ваша задача: переписати ТОЙ САМИЙ запит так, щоб він повертав ІДЕНТИЧНІ рядки,
-- але читав лише 7 партицій. Підказка у MODELS.md (join по сирій партиційній колоні).
-- Перевірте план: EXPLAIN ANALYZE на скомпільованій моделі → «Total Files Read».
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    c.iso_week,
    cat.category,
    COUNT(*) AS events
FROM {{ ref('stg_events') }} e
JOIN {{ ref('calendar') }} c
    ON e.event_date = c.day
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
WHERE c.iso_week = 2
GROUP BY c.iso_week, cat.category
ORDER BY cat.category
