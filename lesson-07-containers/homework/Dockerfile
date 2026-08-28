# Завдання 1 — Dockerfile для ingestor. Специфікація: SPEC.md → «Завдання 1».
#
# Один образ, одна стадія. Що має бути (деталі й бали — у SPEC.md):
#
#   * базовий образ python:3.12-slim з фіксованим тегом (не latest);
#   * PYTHONUNBUFFERED=1 — інакше логів завантаження не видно;
#   * non-root користувач (напр. appuser, uid 1000), USER — перед ENTRYPOINT;
#   * каталог /cache має належати цьому користувачу — туди монтується volume;
#   * залежності з app/requirements.txt ставляться ОКРЕМИМ шаром ПЕРЕД копіюванням
#     коду, з --no-cache-dir (правка коду не має перебудовувати pip install);
#   * ENTRYPOINT в exec-form запускає app/gh_ingest.py.
#
# Перевірте збірку:  docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

# TODO: реалізуйте Dockerfile тут.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 1000 appuser

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /cache && chown appuser /cache

COPY app/gh_ingest.py .

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import os,psycopg; psycopg.connect(host=os.environ['PGHOST'], user=os.environ['PGUSER'], password=os.environ['PGPASSWORD'], dbname=os.environ['PGDATABASE']).close()" || exit 1

USER appuser

ENTRYPOINT ["python", "gh_ingest.py"]

