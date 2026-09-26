{{ config(materialized='table') }}

select
    cast(location_id as int)                                as zone_key,
    coalesce(nullif(trim(zone_name), ''), 'Unknown')        as zone,
    coalesce(nullif(trim(borough), ''), 'Unknown')          as borough,
    coalesce(nullif(trim(service_zone), ''), 'Unknown')     as service_zone
from {{ ref('seed_taxi_zone') }}

union all

select
    -1                                                      as zone_key,
    'Unknown'                                               as zone,
    'Unknown'                                               as borough,
    'Unknown'                                               as service_zone
    