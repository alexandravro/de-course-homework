#!/usr/bin/env bash
# Прогнати job і тести. ./verify.sh solution -> PASS; ./verify.sh tasks (стаби) -> FAIL.
set -euo pipefail
cd "$(dirname "$0")"

IMPL="${1:-tasks}"
case "$IMPL" in
  solution) JOB="../solution/streaming_job.py" ;;
  tasks)    JOB="streaming_job.py" ;;
  *) echo "usage: ./verify.sh [tasks|solution]"; exit 2 ;;
esac

echo ">> [$IMPL] running streaming job: $JOB"
uv run python "$JOB"

echo ">> [$IMPL] pytest"
uv run pytest -q
