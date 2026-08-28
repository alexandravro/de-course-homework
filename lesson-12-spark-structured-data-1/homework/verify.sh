#!/usr/bin/env bash
# Повна перевірка L12: unit-тести функцій → Spark-job → перевірки артефактів.
# Зелений verify.sh = ДЗ зараховано. Запускайте з кореня цієї директорії.
set -uo pipefail

cd "$(dirname "$0")" || exit 1

echo "==> unit-тести функцій (job запускати не треба)"
uv run pytest tests/test_functions.py -q \
    || { echo "FAIL ❌  функції ще не відповідають контрактам зі SPEC.md"; exit 1; }

echo "==> Spark-job: job.py"
uv run python job.py \
    || { echo "FAIL ❌  job впав (див. трейс вище)"; exit 1; }

echo "==> перевірки артефактів у data/output/"
if uv run pytest tests/test_outputs.py -q; then
    echo "PASS ✅  усе зелене: функції, job і контрольні числа."
    exit 0
fi
echo "FAIL ❌  артефакти не зійшлися з checkpoint у SPEC.md."
exit 1
