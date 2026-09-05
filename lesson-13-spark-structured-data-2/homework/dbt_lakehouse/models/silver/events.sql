{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».
-- Джерело: {{ source('bronze', 'raw_events') }}. payload несемо далі сирим рядком — from_json у кроках 2–4.
-- Колонки: event_id, event_type, actor_login, repo_name, repo_owner, created_at,
--          payload, _ingested_at, _source_file
-- Фільтри: 6 типів подій; public = true (NULL відкинути); event_id/repo_name/created_at не null; дедуп по event_id.
-- incremental (append): у is_incremental()-гілці брати лише рядки з _ingested_at > max(_ingested_at) у {{ this }}.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)    as event_id,
    cast(null as string)    as event_type,
    cast(null as string)    as actor_login,
    cast(null as string)    as repo_name,
    cast(null as string)    as repo_owner,
    cast(null as timestamp) as created_at,
    cast(null as string)    as payload,
    cast(null as timestamp) as _ingested_at,
    cast(null as string)    as _source_file
where false
