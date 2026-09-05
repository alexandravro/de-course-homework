-- Крок 9: gold.fact_pull_request. Специфікація: ../../SPEC.md → «Крок 9».
-- Джерело: {{ ref('pull_requests') }}. Грануляція: PR.
-- pr_id = md5(concat_ws('|', repo_name, cast(pr_number as string)));
-- merged_date_id — NULL, якщо не змерджено; label_count через CASE (size(NULL) = -1).
-- Колонки: pr_id, repo_id, author_id, opened_date_id, merged_date_id, state, is_merged, is_draft,
--          additions, deletions, churn, changed_files, commits_count, comments, review_comments,
--          hours_open, label_count.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    cast(null as string)  as pr_id,
    cast(null as string)  as repo_id,
    cast(null as string)  as author_id,
    cast(null as int)     as opened_date_id,
    cast(null as int)     as merged_date_id,
    cast(null as string)  as state,
    cast(null as boolean) as is_merged,
    cast(null as boolean) as is_draft,
    cast(null as int)     as additions,
    cast(null as int)     as deletions,
    cast(null as int)     as churn,
    cast(null as int)     as changed_files,
    cast(null as int)     as commits_count,
    cast(null as int)     as comments,
    cast(null as int)     as review_comments,
    cast(null as double)  as hours_open,
    cast(null as int)     as label_count
where false
