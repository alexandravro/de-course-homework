-- Вітрина = перерахунок із fact_ride «з нуля», рядок у рядок. Ловить застарілі бакети:
-- поїздка змінила зону (ride_started приїхав пізніше), а стара зона не оновилась.
with expected as (
    select
        requested_hour,
        pickup_zone_key,
        count(*)                                                                   as rides_requested,
        count(*) filter (where status = 'completed')                               as rides_completed,
        count(*) filter (where status = 'cancelled')                               as rides_cancelled,
        coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
        coalesce(sum(tip_amount)   filter (where status = 'completed'), 0)::numeric(12, 2) as tips
    from {{ ref('fact_ride') }}
    group by 1, 2
)
select coalesce(a.requested_hour, e.requested_hour) as requested_hour,
       coalesce(a.pickup_zone_key, e.pickup_zone_key) as pickup_zone_key
from {{ ref('agg_zone_hourly') }} as a
full outer join expected as e using (requested_hour, pickup_zone_key)
where a.requested_hour is null
   or e.requested_hour is null
   or a.rides_requested <> e.rides_requested
   or a.rides_completed <> e.rides_completed
   or a.rides_cancelled <> e.rides_cancelled
   or a.gross_revenue   <> e.gross_revenue
   or a.tips            <> e.tips
