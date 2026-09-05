#!/usr/bin/env bash
# Наскрізна перевірка медальйону L13: bronze (PySpark) → silver+gold (dbt на Spark).
# Перевіряє РЕЗУЛЬТАТ: інкрементальність silver.events + кількість рядків + зелений dbt build.
# Зелений verify.sh = ДЗ зараховано. Запускайте з кореня цієї директорії.
set -uo pipefail

cd "$(dirname "$0")" || exit 1

DBT="uv run dbt build --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse"
HOLD=".verify_hold"

restore() { [ -d "$HOLD" ] && mv "$HOLD"/*.json.gz data/landing/ 2>/dev/null; rmdir "$HOLD" 2>/dev/null; }
fail() { echo "FAIL ❌  $1"; restore; exit 1; }
trap restore EXIT

echo "==> Чистий склад (spark-warehouse/, metastore_db/)"
rm -rf spark-warehouse metastore_db derby.log logs dbt_lakehouse/target

# ---------------------------------------------------------------------------
echo "==> Фаза 1: у landing лише перша година → bronze + dbt build"
mkdir -p "$HOLD"
mv data/landing/2024-01-15-13.json.gz data/landing/2024-01-15-14.json.gz "$HOLD"/ || fail "не знайшов landing-файли"

uv run python bronze_job.py >/tmp/l13_b1.log 2>&1 || { tail -20 /tmp/l13_b1.log; fail "bronze (фаза 1)"; }
out=$($DBT 2>&1); echo "$out" | grep -q "ERROR=0" || { echo "$out" | tail -30; fail "dbt build (фаза 1)"; }

n1=$(uv run python -c "from common.spark import build_spark; s=build_spark('v'); print(s.table('silver.events').count()); s.stop()" 2>/dev/null | tail -1)
echo "    silver.events (1 година) = $n1"
[ "$n1" -gt 5000 ] && [ "$n1" -lt 15000 ] || fail "фаза 1: silver.events поза очікуваним діапазоном"

# ---------------------------------------------------------------------------
echo "==> Фаза 2: доносимо решту годин → bronze (append) + dbt build (append, без full-refresh)"
mv "$HOLD"/*.json.gz data/landing/; rmdir "$HOLD"

uv run python bronze_job.py >/tmp/l13_b2.log 2>&1 || { tail -20 /tmp/l13_b2.log; fail "bronze (фаза 2)"; }
out=$($DBT 2>&1); echo "$out" | grep -q "ERROR=0" || { echo "$out" | tail -30; fail "dbt build (фаза 2)"; }

# ---------------------------------------------------------------------------
echo "==> Фаза 3: повторний прогін без нових даних (ідемпотентність)"
uv run python bronze_job.py 2>&1 | grep -q "ідемпотентно" || fail "bronze не ідемпотентний"
out=$($DBT --select silver.events 2>&1); echo "$out" | grep -q "ERROR=0" || { echo "$out" | tail -20; fail "dbt build (фаза 3)"; }

# ---------------------------------------------------------------------------
echo "==> Перевірка кількості рядків у всіх шарах..."
uv run python - <<'PY' || fail "кількість рядків не збіглася з checkpoint (SPEC.md)"
import sys
from common.spark import build_spark
spark = build_spark("l13-verify")
c = {t: spark.table(t).count() for t in [
    "bronze.raw_events", "silver.events", "silver.commits",
    "silver.pull_requests", "silver.issues",
    "gold.dim_repo", "gold.dim_actor", "gold.dim_date",
    "gold.fact_commit", "gold.fact_pull_request", "gold.fact_repo_activity_daily",
]}
distinct_ids = spark.sql("select count(distinct event_id) from silver.events").collect()[0][0]
spark.stop()
for k, v in c.items():
    print(f"   {k:<32} {v}")
expect = {
    "bronze.raw_events": 36000, "silver.events": 30048, "silver.commits": 28237,
    "silver.pull_requests": 1959, "silver.issues": 1704,
    "gold.dim_repo": 16053, "gold.dim_actor": 10514, "gold.dim_date": 3349,
    "gold.fact_commit": 28237, "gold.fact_pull_request": 1959,
    "gold.fact_repo_activity_daily": 13403,
}
ok = all(c[k] == v for k, v in expect.items()) and distinct_ids == 30048
sys.exit(0 if ok else 1)
PY

echo "PASS ✅  silver.events інкрементиться 1 година → 30 048, усі checkpoint-и зійшлися, dbt-тести зелені."
exit 0
