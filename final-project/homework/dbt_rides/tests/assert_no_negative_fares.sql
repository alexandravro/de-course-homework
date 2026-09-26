-- Тест: fare_amount не може бути від'ємним
select ride_id, fare_amount
from {{ ref('rides') }}
where fare_amount is not null
  and fare_amount < 0
