#!/usr/bin/env bash
# End-to-end check for the L16 streaming homework.
#
#   ./verify.sh   # from inside homework/ — expected FAIL until you implement the 6 functions
#
# Brings up the Kafka broker, resets the topic, runs producer.py then
# consumer.py, and runs pytest (offline unit tests + the stats.json checkpoint).
set -uo pipefail
cd "$(dirname "$0")"
RUN="uv run"

echo "==> [1/5] Starting Kafka broker"
docker compose up -d >/dev/null

echo "==> [2/5] Waiting for broker to become healthy"
for i in $(seq 1 30); do
  s=$(docker inspect --format '{{.State.Health.Status}}' hw16-kafka 2>/dev/null || echo missing)
  [[ "$s" == "healthy" ]] && break
  sleep 3
done
if [[ "$(docker inspect --format '{{.State.Health.Status}}' hw16-kafka 2>/dev/null)" != "healthy" ]]; then
  echo "Kafka broker did not become healthy"; docker compose logs --tail 30 kafka; exit 1
fi

echo "==> [3/5] Resetting topic 'github-events'"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic github-events >/dev/null 2>&1 || true
sleep 2
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic github-events \
  --partitions 3 --replication-factor 1 >/dev/null 2>&1 || true

echo "==> [4/5] Running producer + consumer"
rm -f data/output/stats.json
$RUN python producer.py || echo "(producer exited non-zero — expected for stubs)"
$RUN python consumer.py || echo "(consumer exited non-zero — expected for stubs)"

echo "==> [5/5] Running pytest"
$RUN pytest -q
status=$?

echo
if [[ $status -eq 0 ]]; then
  echo "RESULT: PASS"
else
  echo "RESULT: FAIL"
fi
exit $status
