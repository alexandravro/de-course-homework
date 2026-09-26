{{ config(tags=['reconcile']) }}
-- Факт — дзеркало silver.rides: ті самі поїздки, статуси й суми. Ловить інкремент, який
-- не підхопив зміну (запізніла подія оновила Silver, а факт лишився старим).
select coalesce(f.ride_id, r.ride_id) as ride_id
from {{ ref('fact_ride') }} as f
full outer join {{ ref('rides') }} as r using (ride_id)
where f.ride_id is null
   or r.ride_id is null
   or f.status is distinct from r.status
   or f.total_amount is distinct from r.total_amount
   or f.paid_at is distinct from r.paid_at
   or f.pickup_zone_key is distinct from r.pickup_zone_id
