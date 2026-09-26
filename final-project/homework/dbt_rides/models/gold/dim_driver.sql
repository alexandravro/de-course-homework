{{ config(materialized='table') }}

with all_accepted as (
    select *
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
),

drivers as (
    select distinct on (payload->'driver'->>'id')
        payload->'driver'->>'id'                            as driver_id,
        (payload->'driver'->>'rating')::numeric             as rating,
        payload->'driver'->'vehicle'->>'type'               as vehicle_type,
        payload->'driver'->'vehicle'->>'medallion'          as medallion
    from all_accepted
    where payload->'driver'->>'id' is not null
    order by (payload->'driver'->>'id'), occurred_at desc
)

select
    md5(driver_id)                                          as driver_key,
    driver_id,
    rating,
    vehicle_type,
    medallion
from drivers

union all

select
    'unknown'                                               as driver_key,
    'unknown'                                               as driver_id,
    null::numeric                                           as rating,
    'unknown'                                               as vehicle_type,
    null::text                                              as medallion
    