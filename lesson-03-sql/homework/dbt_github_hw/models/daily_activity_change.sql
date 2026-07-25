-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
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
    LAG(events) OVER (ORDER BY event_date) AS prev_day_events,
    events - LAG(events) OVER (ORDER BY event_date) AS delta_events
FROM daily
ORDER BY event_date