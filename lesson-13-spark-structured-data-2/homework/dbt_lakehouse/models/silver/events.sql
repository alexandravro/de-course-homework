{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».
-- Джерело: {{ source('bronze', 'raw_events') }}. payload несемо далі сирим рядком — from_json у кроках 2–4.
-- Колонки: event_id, event_type, actor_login, repo_name, repo_owner, created_at,
--          payload, _ingested_at, _source_file
-- Фільтри: 6 типів подій; public = true (NULL відкинути); event_id/repo_name/created_at не null; дедуп по event_id.
-- incremental (append): у is_incremental()-гілці брати лише рядки з _ingested_at > max(_ingested_at) у {{ this }}.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
{{ config(materialized='incremental', incremental_strategy='append') }}

with source as (
    select *
    from {{ source('bronze', 'raw_events') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        id                                          as event_id,
        type                                        as event_type,
        actor.login                                 as actor_login,
        repo.name                                   as repo_name,
        split(repo.name, '/')[0]                    as repo_owner,
        to_timestamp(created_at)                    as created_at,
        payload                                     as payload,
        _ingested_at,
        _source_file
    from source
    where type in ('PushEvent','PullRequestEvent','IssuesEvent','IssueCommentEvent','WatchEvent','ForkEvent')
      and public = true
      and id is not null
      and repo.name is not null
      and created_at is not null
),

deduped as (
    select *,
        row_number() over (partition by event_id order by _ingested_at) as rn
    from cleaned
)

select
    event_id, event_type, actor_login, repo_name, repo_owner,
    created_at, payload, _ingested_at, _source_file
from deduped
where rn = 1

