# Образ демо-застосунку. Кожен рядок — окреме рішення; пояснення поряд.

# Тег фіксуємо: latest — рухома ціль, завтра той самий build дасть інший Python.
# slim, бо Airflow/Spark-образи теж Debian/glibc — одна libc на весь стек.
FROM python:3.12-slim

# PYTHONUNBUFFERED=1 — stdout не буферизується, коли він не термінал. Без цього
#   `docker logs` мовчить, поки процес не завершиться: класична причина
#   «контейнер висить, логів нема».
# PYTHONDONTWRITEBYTECODE=1 — не писати .pyc: перевикористати їх нікому,
#   контейнер живе один запуск.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Користувач створюється до коду: шар не змінюється ніколи, тому стоїть вище.
# uid 1000 — перший користувач на типовому Linux-хості: файли в bind mount не
# стають чужими.
RUN useradd --create-home --uid 1000 appuser

WORKDIR /app

# Залежності — окремим шаром ПЕРЕД кодом. Поки requirements.txt не змінився,
# pip install береться з кешу, скільки б разів ми не правили main.py.
# --no-cache-dir: pip-кеш в образі не має споживача, другого install не буде.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Явний список файлів (whitelist), а не `COPY . .` + .dockerignore (blacklist):
# забути рядок в .dockerignore легко й тихо, забути файл тут — видно одразу.
COPY main.py events.csv ./

# USER — після всіх операцій, що потребують root (useradd, pip install).
# Код лишається власністю root: appuser читає й виконує, але не переписує.
USER appuser

# Exec-form (JSON-масив): PID 1 — сам python, а не /bin/sh, який ковтає сигнали.
ENTRYPOINT ["python", "main.py"]
