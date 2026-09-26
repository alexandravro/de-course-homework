-- Кожна поїздка з ride_requested у silver.events є в silver.rides — і навпаки.
with requested as (
    select distinct ride_id from {{ ref('events') }} where event_type = 'ride_requested'
)
select coalesce(q.ride_id, r.ride_id) as ride_id
from requested as q
full outer join {{ ref('rides') }} as r using (ride_id)
where q.ride_id is null or r.ride_id is null
