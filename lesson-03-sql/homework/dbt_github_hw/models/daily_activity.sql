-- =====================================================================
-- TASK 3 — daily_activity (12 балів). Специфікація: ../../MODELS.md → «daily_activity».
-- Кількість подій по днях + накопичувальний підсумок: SUM(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
WITH daily AS (
    SELECT
        event_date,
        COUNT(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
)

SELECT
    event_date,
    events,
    SUM(events) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_events
FROM daily
ORDER BY event_date
