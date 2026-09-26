{{ config(tags=['reconcile']) }}
-- Жодна валідна подія не зникає між Bronze і Silver: унікальні event_id, що пройшли
-- правила Silver, мають збігатися за кількістю з рядками silver.events.
with bronze as (
    select count(distinct event_id) as n
    from {{ source('bronze', 'raw_events') }}
    where source is distinct from 'loadtest'
      and occurred_at is not null
      and ride_id is not null
),
silver as (
    select count(*) as n from {{ ref('events') }}
)
select bronze.n as bronze_events, silver.n as silver_events
from bronze cross join silver
where bronze.n <> silver.n
