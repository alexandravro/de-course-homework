{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert',
) }}

with rides as (
    select *
    from {{ ref('rides') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark('_ingested_at') }}
    {% endif %}
)

select
    r.ride_id,
    coalesce(dpu.zone_key, -1)                              as pickup_zone_key,
    coalesce(ddo.zone_key, -1)                              as dropoff_zone_key,
    coalesce(dd.driver_key, 'unknown')                      as driver_key,
    r.status,
    r.requested_at,
    date_trunc('hour', r.requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,
    (r.requested_at at time zone 'UTC')::date               as requested_date,
    r.rider_id,
    r.rider_platform,
    r.requested_vehicle,
    r.surge_estimate,
    r.accepted_at,
    r.eta_seconds,
    r.started_at,
    r.completed_at,
    r.distance_km,
    r.fare_amount,
    r.surge_multiplier,
    r.tolls_amount,
    r.tip_amount,
    r.total_amount,
    r.currency,
    r.cancelled_at,
    r.cancelled_by,
    r.cancel_reason,
    r.cancel_stage,
    r.paid_at,
    r.payment_method,
    r.payment_amount,
    r.wait_seconds,
    r.trip_seconds,
    r._ingested_at
from rides r
left join {{ ref('dim_zone') }} dpu on r.pickup_zone_id = dpu.zone_key
left join {{ ref('dim_zone') }} ddo on r.dropoff_zone_id = ddo.zone_key
left join {{ ref('dim_driver') }} dd on r.driver_id = dd.driver_id
