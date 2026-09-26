# Lesson 16 — Kafka demo stack (instructor)

Self-contained demo for the Kafka teaching stack. This is the **rich** demo
infrastructure — deliberately richer than the homework (which uses one broker +
plain JSON). The step-by-step runbook is in `../demo.md`. CDC (Kafka Connect +
Debezium + Postgres) is not part of this stack — it stays conceptual, covered in
`knowledge.md` §7 and `lecturer_knowledge.md`.

## What is here

| File | Role |
|---|---|
| `docker-compose.yml` | Kafka (KRaft, no ZooKeeper) + Schema Registry + Kafka UI |
| `producer.py` | NYC Taxi replay producer, Avro via Schema Registry |
| `consumer.py` | Consumer with offset / consumer-group rebalance logging |
| `avro_roundtrip.py` | Avro as a plain `.avro` file on disk — no Kafka, no network |
| `schemas/trip.avsc` | Avro schema for the `taxi-trips` topic |
| `schemas/trip_v2.avsc` | Evolved schema (adds nullable `payment_type`) for the compatibility demo |

## Ports

| Port | Service |
|---|---|
| 9092 | Kafka broker (host listener) |
| 8081 | Schema Registry |
| 8082 | Kafka UI (http://localhost:8082) |

## Prerequisites

- Docker + Docker Compose
- `uv` for Python
- `jq` and `curl` (used in the demo for Schema Registry compatibility calls)
- The January 2024 Yellow Taxi Parquet in the shared course data dir at
  `../../data/source/yellow_tripdata_2024-01.parquet` (lesson 02 downloads it;
  it is the shared course dataset, reused across lessons).

## Quick start

Run all commands from this `code/` dir.

```bash
docker compose up -d

# Create the topic (auto-create is disabled in the broker config)
docker exec l16-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic taxi-trips --partitions 3 --replication-factor 1

uv run producer.py
uv run consumer.py
```

No CLI flags on either script — the run parameters (speed, max events, group id, ...) are
plain constants at the top of each file. Edit them there for a different run.

Teardown: `docker compose down -v`
