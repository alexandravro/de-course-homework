-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * incremental, unique_key='ride_id', delete+insert
--   * перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--   * поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--   * status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--   * _ingested_at = максимум _ingested_at усіх подій поїздки

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert',
) }}

with all_events as (
    select *
    from {{ ref('events') }}
    {% if is_incremental() %}
    where ride_id in (
        select distinct ride_id
        from {{ ref('events') }}
        where _ingested_at > {{ high_watermark('_ingested_at') }}
    )
    {% endif %}
),

requested as (
    select
        ride_id,
        occurred_at                                         as requested_at,
        (payload->>'rider')::jsonb->>'id'                   as rider_id,
        (payload->>'rider')::jsonb->>'platform'             as rider_platform,
        (payload->>'pickup')::jsonb->>'zone_id'             as requested_pickup_zone_id,
        (payload->>'dropoff')::jsonb->>'zone_id'            as requested_dropoff_zone_id,
        payload->>'requested_vehicle'                       as requested_vehicle,
        (payload->>'surge_estimate')::numeric               as surge_estimate,
        _ingested_at
    from all_events
    where event_type = 'ride_requested'
),

accepted as (
    select
        ride_id,
        occurred_at                                         as accepted_at,
        (payload->>'driver')::jsonb->>'id'                  as driver_id,
        (payload->>'driver')::jsonb->>'rating'              as driver_rating,
        (payload->>'driver')::jsonb->'vehicle'->>'type'     as vehicle_type,
        (payload->>'eta_seconds')::int                      as eta_seconds
    from all_events
    where event_type = 'ride_accepted'
),

started as (
    select
        ride_id,
        occurred_at                                         as started_at,
        (payload->>'pickup')::jsonb->>'zone_id'             as actual_pickup_zone_id,
        (payload->>'odometer_km')::numeric                  as odometer_start_km
    from all_events
    where event_type = 'ride_started'
),

completed as (
    select
        ride_id,
        occurred_at                                         as completed_at,
        (payload->>'dropoff')::jsonb->>'zone_id'            as actual_dropoff_zone_id,
        (payload->>'distance_km')::numeric                  as distance_km,
        (payload->'fare'->>'amount')::numeric               as fare_amount,
        (payload->'fare'->>'surge_multiplier')::numeric     as surge_multiplier,
        (payload->'fare'->>'tolls')::numeric                as tolls_amount,
        (payload->'fare'->>'tip')::numeric                  as tip_amount,
        (payload->'fare'->>'total')::numeric                as total_amount,
        payload->'fare'->>'currency'                        as currency
    from all_events
    where event_type = 'ride_completed'
),

cancelled as (
    select
        ride_id,
        occurred_at                                         as cancelled_at,
        payload->>'cancelled_by'                            as cancelled_by,
        payload->>'reason'                                  as cancel_reason,
        payload->>'stage'                                   as cancel_stage
    from all_events
    where event_type = 'ride_cancelled'
),

payment as (
    select
        ride_id,
        occurred_at                                         as paid_at,
        payload->'payment'->>'method'                       as payment_method,
        (payload->'payment'->>'amount')::numeric            as payment_amount
    from all_events
    where event_type = 'payment_captured'
),

joined as (
    select
        r.ride_id,
        date_trunc('day', r.requested_at)::date             as requested_date,
        r.requested_at,
        r.rider_id,
        r.rider_platform,
        r.requested_vehicle,
        r.surge_estimate,
        r.requested_pickup_zone_id,
        r.requested_dropoff_zone_id,
        a.accepted_at,
        a.driver_id,
        a.vehicle_type,
        a.eta_seconds,
        s.started_at,
        coalesce(s.actual_pickup_zone_id, r.requested_pickup_zone_id)::integer   as pickup_zone_id,
        s.odometer_start_km,
        c.completed_at,
        coalesce(c.actual_dropoff_zone_id, r.requested_dropoff_zone_id)::integer as dropoff_zone_id,
        c.distance_km,
        c.fare_amount,
        c.surge_multiplier,
        c.tolls_amount,
        c.tip_amount,
        c.total_amount,
        c.currency,
        cx.cancelled_at,
        cx.cancelled_by,
        cx.cancel_reason,
        cx.cancel_stage,
        p.paid_at,
        p.payment_method,
        p.payment_amount,
        case
            when cx.cancelled_at is not null then 'cancelled'
            when c.completed_at is not null  then 'completed'
            when s.started_at is not null    then 'in_progress'
            when a.accepted_at is not null   then 'accepted'
            else                                  'requested'
        end                                                             as status,
        extract(epoch from (a.accepted_at - r.requested_at))::int      as wait_seconds,
        extract(epoch from (c.completed_at - s.started_at))::int       as trip_seconds,
        (
            select max(_ingested_at)
            from all_events e2
            where e2.ride_id = r.ride_id
        )                                                               as _ingested_at
    from requested r
    left join accepted  a  on r.ride_id = a.ride_id
    left join started   s  on r.ride_id = s.ride_id
    left join completed c  on r.ride_id = c.ride_id
    left join cancelled cx on r.ride_id = cx.ride_id
    left join payment   p  on r.ride_id = p.ride_id
)

select * from joined
