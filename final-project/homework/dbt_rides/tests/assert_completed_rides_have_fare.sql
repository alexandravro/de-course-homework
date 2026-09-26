-- Тест: completed поїздки повинні мати fare_amount
select ride_id, status, fare_amount
from {{ ref('rides') }}
where status = 'completed'
  and fare_amount is null
