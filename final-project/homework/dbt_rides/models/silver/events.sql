-- silver.events — ЕТАП 2. Grain: одна подія (event_id). SPEC.md, розділ 4.2.
--   * incremental, unique_key='event_id', delete+insert; межа — high_watermark() за _ingested_at
--   * відкинути source='loadtest' і рядки без occurred_at / ride_id
--   * один рядок на event_id (найраніший _ingested_at, за рівності — _source_file)
--   * payload text -> jsonb; occurred_date = utc_date(occurred_at); _loaded_at = run_started_at

{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
) }}

with source as (
    select *
    from {{ source('bronze', 'raw_events') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark('_ingested_at') }}
    {% endif %}
),

filtered as (
    select *
    from source
    where source != 'loadtest'
      and occurred_at is not null
      and ride_id is not null
),

deduped as (
    select distinct on (event_id)
        event_id,
        event_type,
        ride_id,
        occurred_at,
        source,
        payload::jsonb as payload,
        (occurred_at at time zone 'UTC')::date as occurred_date,
        _ingested_at,
        _source_file
    from filtered
    order by event_id, _ingested_at, _source_file
),

final as (
    select
        event_id,
        event_type,
        ride_id,
        occurred_at,
        occurred_date,
        source,
        payload,
        _ingested_at,
        now() as _loaded_at
    from deduped
)

select * from final

