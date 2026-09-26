-- Статус узгоджений із віхами, віхи йдуть у логічному порядку.
select ride_id, status, requested_at, accepted_at, started_at, completed_at, cancelled_at, paid_at
from {{ ref('rides') }}
where (status = 'completed' and completed_at is null)
   or (status = 'cancelled' and cancelled_at is null)
   or (completed_at is not null and cancelled_at is not null)
   or (accepted_at  < requested_at)
   or (started_at   < accepted_at)
   or (completed_at < started_at)
   or (paid_at      < completed_at)
