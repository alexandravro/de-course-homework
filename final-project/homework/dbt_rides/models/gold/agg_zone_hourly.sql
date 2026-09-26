{{ config(
    materialized='incremental',
    unique_key='agg_key',
    incremental_strategy='delete+insert',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }};{% endif %}"
) }}

select
    md5(concat_ws('|',
        (date_trunc('hour', requested_at at time zone 'UTC') at time zone 'UTC')::text,
        pickup_zone_key::text
    ))                                                              as agg_key,
    date_trunc('hour', requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,
    pickup_zone_key,
    count(*)                                                        as rides_requested,
    count(*) filter (where status = 'completed')                    as rides_completed,
    count(*) filter (where status = 'cancelled')                    as rides_cancelled,
    coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12,2) as gross_revenue,
    coalesce(sum(tip_amount) filter (where status = 'completed'), 0)::numeric(12,2)   as tips,
    avg(wait_seconds)                                               as avg_wait_seconds,
    avg(trip_seconds)                                               as avg_trip_seconds,
    avg(surge_multiplier)                                           as avg_surge,
    max(_ingested_at)                                               as _ingested_at
from {{ ref('fact_ride') }}
group by date_trunc('hour', requested_at at time zone 'UTC') at time zone 'UTC', pickup_zone_key
