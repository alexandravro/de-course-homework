-- Grain вітрини — (requested_hour, pickup_zone_key): дублікатів бути не може.
select requested_hour, pickup_zone_key, count(*) as n
from {{ ref('agg_zone_hourly') }}
group by 1, 2
having count(*) > 1
