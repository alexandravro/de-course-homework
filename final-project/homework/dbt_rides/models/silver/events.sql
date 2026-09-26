-- silver.events — ЕТАП 2. Grain: одна подія (event_id). SPEC.md, розділ 4.2.
--   * incremental, unique_key='event_id', delete+insert; межа — high_watermark() за _ingested_at
--   * відкинути source='loadtest' і рядки без occurred_at / ride_id
--   * один рядок на event_id (найраніший _ingested_at, за рівності — _source_file)
--   * payload text -> jsonb; occurred_date = utc_date(occurred_at); _loaded_at = run_started_at

{{ exceptions.raise_compiler_error("TODO: реалізуйте модель events (див. SPEC.md)") }}
