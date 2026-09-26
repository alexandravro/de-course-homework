{{ config(tags=['reconcile']) }}
-- Календарні дати Silver і Gold — за UTC, а не за часовим поясом сесії клієнта.
-- Тести dbt запускаються з PGTZ=Pacific/Chatham (UTC+13:45): якщо модель робить
-- `timestamptz::date` або `date_trunc('hour', ts)` без явного UTC, її дати й години поїдуть.
select event_id as id, occurred_date, (occurred_at at time zone 'UTC')::date as expected
from {{ ref('events') }}
where occurred_date <> (occurred_at at time zone 'UTC')::date

union all

select ride_id, requested_date, (requested_at at time zone 'UTC')::date
from {{ ref('fact_ride') }}
where requested_date <> (requested_at at time zone 'UTC')::date

union all

select ride_id, null, null
from {{ ref('fact_ride') }}
where requested_hour <> date_trunc('hour', requested_at at time zone 'UTC') at time zone 'UTC'
