# Standalone Avro round-trip — no Kafka, no Schema Registry, no network.
#
# Shows the OTHER way Avro carries its schema: an Object Container File
# (.avro), where the schema travels once in the file header instead of once
# per message (that per-message case is what producer.py/consumer.py do,
# via Schema Registry — see knowledge.md §6 for the two side by side).
#
# Writes N synthetic trips to a local .avro file, deserializes them back at
# runtime, and compares the on-disk size against the same records as JSON.
#
# Run: uv run avro_roundtrip.py

import json
import os
import random

import fastavro
from icecream import ic

SCHEMA_PATH = "schemas/trip.avsc"
OUT_PATH = "taxi_trips_sample.avro"
N_RECORDS = 1000


def make_records(n: int) -> list[dict]:
    records = []
    pickup_ts = 1_705_327_200_000  # 2024-01-15T14:00:00Z
    for _ in range(n):
        pickup_ts += 60_000  # one trip a minute
        records.append({
            "pickup_ts": pickup_ts,
            "dropoff_ts": pickup_ts + random.randint(300_000, 1_800_000),
            "pu_location_id": random.randint(1, 263),
            "do_location_id": random.randint(1, 263),
            "fare_amount": round(random.uniform(5, 80), 2),
            "tip_amount": round(random.uniform(0, 15), 2),
            "passenger_count": random.randint(1, 4),
        })
    return records


def main():
    with open(SCHEMA_PATH) as f:
        schema = fastavro.parse_schema(json.load(f))
    records = make_records(N_RECORDS)

    # Write: schema header once, then binary records back to back — field
    # names are never repeated, only the values, in the order the schema says.
    with open(OUT_PATH, "wb") as f:
        fastavro.writer(f, schema, records)

    avro_size = os.path.getsize(OUT_PATH)
    json_size = sum(len(json.dumps(r).encode("utf-8")) + 1 for r in records)  # +1 per newline
    ic(avro_size, avro_size / N_RECORDS, json_size, json_size / N_RECORDS)

    # Read back: deserialize at runtime. No schema is passed in here — fastavro
    # reads it straight from the file's own header, the way any Avro reader would.
    with open(OUT_PATH, "rb") as f:
        read_back = list(fastavro.reader(f))

    ic(read_back[0])

    # The schema marks pickup_ts/dropoff_ts as logicalType "timestamp-millis",
    # so fastavro hands them back as datetime objects, not raw ints — the
    # logical type is decoded automatically on the way out. Convert back to
    # millis to check the round-trip preserved the original values exactly.
    def to_millis(dt):
        return int(dt.timestamp() * 1000)

    for original, back in zip(records, read_back):
        assert to_millis(back["pickup_ts"]) == original["pickup_ts"]
        assert to_millis(back["dropoff_ts"]) == original["dropoff_ts"]
        assert {k: v for k, v in back.items() if k not in ("pickup_ts", "dropoff_ts")} == \
               {k: v for k, v in original.items() if k not in ("pickup_ts", "dropoff_ts")}
    ic("Round-trip OK: all fields match (timestamps decoded back to datetime by the logical type).")

    ic(f"Inspect the raw bytes with: xxd {OUT_PATH} | head")


if __name__ == "__main__":
    main()
