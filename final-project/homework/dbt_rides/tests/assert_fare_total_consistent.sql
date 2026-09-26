-- total = тариф × surge + збори + чайові (NULL-чайові = 0). Множник береться ФАКТИЧНИЙ
-- (surge_multiplier із ride_completed), а не оцінка із замовлення (surge_estimate).
select ride_id, fare_amount, surge_multiplier, tolls_amount, tip_amount, total_amount
from {{ ref('rides') }}
where status = 'completed'
  and abs(total_amount - (fare_amount * surge_multiplier + tolls_amount + coalesce(tip_amount, 0))) > 0.01
