-- gold.fact_ride — ЕТАП 2. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
--   * incremental, unique_key='ride_id', delete+insert; беріть з ref('rides') те, що змінилося
--   * FK до вимірів через LEFT JOIN + COALESCE (-1 / 'unknown'): рядок не губиться
--   * requested_date = utc_date(requested_at); requested_hour = початок години за UTC

{{ exceptions.raise_compiler_error("TODO: реалізуйте модель fact_ride (див. SPEC.md)") }}
