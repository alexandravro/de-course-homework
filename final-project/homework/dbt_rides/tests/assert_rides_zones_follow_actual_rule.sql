-- Правило зон: те, що СТАЛОСЯ (ride_started / ride_completed), перекриває те, що ЗАМОВИЛИ.
-- Перераховуємо очікуване напряму з silver.events — окремо від логіки моделі rides.
with ev as (
    select
        ride_id,
        max((payload #>> '{pickup,zone_id}')::int)  filter (where event_type = 'ride_requested') as req_pu,
        max((payload #>> '{pickup,zone_id}')::int)  filter (where event_type = 'ride_started')   as act_pu,
        max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_requested') as req_do,
        max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_completed') as act_do
    from {{ ref('events') }}
    group by ride_id
)
select r.ride_id, r.pickup_zone_id, r.dropoff_zone_id, ev.req_pu, ev.act_pu, ev.req_do, ev.act_do
from {{ ref('rides') }} as r
join ev using (ride_id)
where r.pickup_zone_id  is distinct from coalesce(ev.act_pu, ev.req_pu)
   or r.dropoff_zone_id is distinct from coalesce(ev.act_do, ev.req_do)
