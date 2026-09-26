#!/usr/bin/env bash
# Ідемпотентний запуск локального стеку: виконуйте скільки завгодно разів, результат той самий.
#
#   ./up.sh                   postgres + kafka + producer + consumer (стрім працює сам)
#   ./up.sh --airflow         + Airflow; чекає, поки DAG зареєструється, і вмикає його
#   ./up.sh --airflow --run   + один запуск DAG одразу; чекає на його завершення
#   ./up.sh --down            зупинити все, ДАНІ ЗБЕРЕГТИ
#   ./up.sh --reset           знести все разом із даними (томи й data/landing): чистий старт
#
# Що саме ідемпотентне:
#   * .env створюється лише якщо його ще нема;
#   * `docker compose up -d` нічого не перестворює, якщо конфігурація не змінилась;
#   * producer не публікує корпус повторно, якщо топік уже непорожній;
#   * DAG вмикається лише тоді, коли він на паузі (і лише коли вже зареєстрований у метабазі:
#     `airflow dags unpause` раніше за реєстрацію мовчки нічого не робить, а DAG потім
#     з'являється на паузі).
set -euo pipefail
cd "$(dirname "$0")"

DAG_ID=rides_medallion
WITH_AIRFLOW=0
RUN_DAG=0
ACTION=up
for arg in "$@"; do
    case "$arg" in
        --airflow) WITH_AIRFLOW=1 ;;
        --run) WITH_AIRFLOW=1; RUN_DAG=1 ;;
        --down) ACTION=down ;;
        --reset) ACTION=reset ;;
        *) echo "використання: ./up.sh [--airflow] [--run] | --down | --reset"; exit 2 ;;
    esac
done

airflow() { docker compose exec -T airflow-scheduler airflow "$@"; }
# Метабаза Airflow — окрема база на тому самому Postgres.
meta() { docker compose exec -T postgres psql -U airflow -d airflow -tA -c "$1"; }

if [ "$ACTION" = down ]; then
    docker compose --profile airflow down --remove-orphans
    exit 0
fi
if [ "$ACTION" = reset ]; then
    docker compose --profile airflow down -v --remove-orphans
    # landing лежить на диску хоста, `down -v` його не чіпає: без очищення consumer дописав би
    # ті самі події вдруге під іншими іменами.
    find data/landing -mindepth 1 ! -name .gitkeep -delete 2>/dev/null || true
    echo "скинуто: томи й data/landing очищено"
    exit 0
fi

# uid/gid хоста: файли landing належатимуть вам, а не root (важливо на Linux / WSL).
if [ ! -f .env ]; then
    printf 'LOCAL_UID=%s\nLOCAL_GID=%s\n' "$(id -u)" "$(id -g)" > .env
    echo "створено .env (LOCAL_UID=$(id -u), LOCAL_GID=$(id -g))"
fi

echo "==> стрімінг-стек: postgres, kafka, producer, consumer"
docker compose up -d --wait postgres kafka
docker compose up -d

if [ "$WITH_AIRFLOW" = 1 ]; then
    echo "==> Airflow"
    docker compose --profile airflow up -d --build
    echo "    чекаю, поки DAG $DAG_ID зареєструється в метабазі (перший раз ~1 хв)…"
    paused=""
    for _ in $(seq 1 90); do
        paused=$(meta "select is_paused from dag where dag_id = '$DAG_ID'" 2>/dev/null || true)
        [ -n "$paused" ] && break
        sleep 4
    done
    if [ -z "$paused" ]; then
        echo "DAG $DAG_ID не зареєструвався за 6 хв. Дивіться: docker compose logs airflow-scheduler"
        exit 1
    fi
    if [ "$paused" = t ]; then
        airflow dags unpause "$DAG_ID" > /dev/null
        echo "    DAG $DAG_ID увімкнено (був на паузі)"
    else
        echo "    DAG $DAG_ID уже увімкнено"
    fi
fi

if [ "$RUN_DAG" = 1 ]; then
    echo "==> один запуск DAG зараз"
    airflow dags trigger "$DAG_ID" > /dev/null
    state=""
    for _ in $(seq 1 150); do
        state=$(meta "select state from dag_run where dag_id = '$DAG_ID' order by id desc limit 1")
        case "$state" in
            success) echo "    запуск завершився: success"; break ;;
            failed) echo "    запуск впав (failed). Дивіться логи задач в Airflow UI"; exit 1 ;;
        esac
        sleep 4
    done
    [ "$state" = success ] || { echo "запуск не завершився за 10 хв"; exit 1; }
fi

echo
docker compose --profile airflow ps --format 'table {{.Name}}\t{{.Status}}'
echo
echo "Kafka: localhost:9094 · Postgres: localhost:5433 (taxi/taxi, база taxi_dwh)"
[ "$WITH_AIRFLOW" = 1 ] && echo "Airflow: http://localhost:8080 (airflow / airflow)"
exit 0
