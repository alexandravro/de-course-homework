#!/usr/bin/env bash
# Перевірка проєкту. Зелений verify.sh = етап зараховано. Запускайте з кореня цієї директорії.
#
#   ./verify.sh lint           ruff + mypy (обовʼязкова умова для будь-якого етапу)
#   ./verify.sh ingest         етап 1: consumer (Kafka -> landing) і bronze_job (Spark -> Bronze)
#   ./verify.sh transform      етап 2: dbt Silver і Gold, інкремент, incremental == full refresh
#   ./verify.sh orchestrate    етап 3: DAG у Airflow
#   ./verify.sh                усе разом
#
# Доки в етапі лишились заглушки, скрипт зупиняється одразу: Docker, Spark і dbt не запускаються.
# Етап 3 збирає все разом (ваші bronze_job.py і dbt-моделі), тому потребує готових етапів 1 і 2.
# Етапи 1 і 2 потребують Postgres і Kafka з docker compose (скрипт підніме їх сам).
# Перед ними ЗУПИНІТЬ Airflow: він пише в ті самі таблиці, що й тести.
set -uo pipefail

cd "$(dirname "$0")" || exit 1

STAGE="${1:-all}"

fail() { echo "FAIL ❌  $1"; exit 1; }

# Без рішення нічого не запускаємо: жодного Docker, Spark чи dbt, поки в етапі лишились заглушки.
precheck() { uv run python -m scripts.precheck "$@" || fail "рішення ще нема: замініть заглушки (перелік вище)"; }

need_infra() {
    echo "==> docker compose: Postgres і Kafka"
    docker compose up -d --wait postgres kafka >/dev/null 2>&1 \
        || fail "не вдалося підняти Postgres і Kafka: docker compose up -d"
}

airflow_must_be_stopped() {
    if [ -n "$(docker ps -q --filter name=fp-airflow-scheduler)" ]; then
        fail "Airflow запущений і пише в ті самі таблиці, що й тести: docker compose --profile airflow stop"
    fi
}

stage_lint() {
    echo "==> lint: ruff check";         uv run ruff check .        || fail "ruff check"
    echo "==> lint: ruff format --check"; uv run ruff format --check . || fail "ruff format"
    echo "==> lint: mypy";                uv run mypy               || fail "mypy"
}

stage_ingest() {
    precheck ingest
    echo "==> етап 1: unit-тести consumer-а, корпусу й перевірки заглушок (Docker не потрібен)"
    uv run pytest tests/test_corpus.py tests/test_consumer.py tests/test_precheck.py -q \
        || fail "consumer: unit-тести"
    need_infra; airflow_must_be_stopped
    echo "==> етап 1: Kafka e2e (усі повідомлення в landing; kill -9 не губить нічого)"
    uv run pytest tests/test_stream_e2e.py -q \
        || fail "consumer: e2e на живому Kafka"
    echo "==> етап 1: Bronze — Spark на фікстурах (схема, payload, ідемпотентність, атомарність)"
    uv run pytest tests/test_bronze.py -q \
        || fail "bronze_job"
}

stage_transform() {
    precheck transform
    echo "==> етап 2: dbt — кожен тест виконується кроком DAG-у (Docker не потрібен)"
    uv run pytest tests/test_dbt_selection.py -q \
        || fail "dbt: є тест, який не виконує жоден крок DAG-у, або моделі не збираються"
    need_infra; airflow_must_be_stopped
    echo "==> етап 2: dbt — інкремент батч за батчем; ідемпотентність; incremental == full refresh;"
    echo "    дати за UTC при нестандартному поясі сесії"
    uv run pytest tests/test_incremental.py -q \
        || fail "dbt-моделі не відповідають SPEC.md (контрольні числа, інкремент або власні тести)"
}

stage_orchestrate() {
    precheck orchestrate
    need_infra
    echo "==> етап 3: Airflow — структура DAG-у і запуски поспіль"
    uv run pytest tests/test_orchestrate.py -q \
        || fail "DAG rides_medallion"
}

case "$STAGE" in
    lint)        stage_lint ;;
    ingest)      stage_lint; stage_ingest ;;
    transform)   stage_lint; stage_transform ;;
    orchestrate) stage_lint; stage_orchestrate ;;
    all)         stage_lint; precheck orchestrate; stage_ingest; stage_transform; stage_orchestrate ;;
    *)           echo "використання: ./verify.sh [lint|ingest|transform|orchestrate|all]"; exit 2 ;;
esac

echo "PASS ✅  ${STAGE}: усе зелене."
