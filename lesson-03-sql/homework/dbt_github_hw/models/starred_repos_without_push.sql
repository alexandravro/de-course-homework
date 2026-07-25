-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent: anti-join (NOT EXISTS).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT DISTINCT repo_name
FROM {{ ref('stg_events') }} AS w
WHERE event_type = 'WatchEvent'
  AND NOT EXISTS (
      SELECT 1
      FROM {{ ref('stg_events') }} AS p
      WHERE p.event_type = 'PushEvent'
        AND p.repo_name = w.repo_name
  )