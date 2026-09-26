-- Сума, списана billing-ом, дорівнює сумі з ride_completed (де відомі обидві).
select ride_id, total_amount, payment_amount
from {{ ref('fact_ride') }}
where payment_amount is not null
  and total_amount   is not null
  and payment_amount <> total_amount
