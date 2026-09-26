#!/usr/bin/env bash
# Обгортка над dbt: підставляє шляхи до проєкту й профілю та виносить target/ і logs/ у /tmp.
#   ./dbt.sh build --selector silver --indirect-selection cautious
set -euo pipefail
cd "$(dirname "$0")"
export DBT_TARGET_PATH="${DBT_TARGET_PATH:-/tmp/fp-dbt-target}"
export DBT_LOG_PATH="${DBT_LOG_PATH:-/tmp/fp-dbt-logs}"
exec uv run dbt "$@" --project-dir dbt_rides --profiles-dir dbt_rides
