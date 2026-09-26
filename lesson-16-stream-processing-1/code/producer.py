# NYC TLC Yellow Taxi replay producer.
#
# Reads the January 2024 Yellow Taxi Parquet, sorts trips by pickup time, and
# publishes each trip to the Kafka topic "taxi-trips" as an Avro message
# serialized through Schema Registry. The message key is the pickup location id,
# so all trips from one zone land in the same partition (local ordering per zone).
#
# Run (with the demo stack up):  uv run producer.py
#
# No CLI flags — the knobs below are plain constants. Edit them in the file
# for a different run (e.g. LATE_EVENT_FRACTION > 0 for the L17 watermark demo)
# instead of passing arguments.

import random
import time

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from icecream import ic

PARQUET_PATH = "../../data/source/yellow_tripdata_2024-01.parquet"
TOPIC = "taxi-trips"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_PATH = "schemas/trip.avsc"

MAX_EVENTS = 500  # 0 = replay the whole file
SPEED = 1000  # compress event-time into wall-clock time; see note below
LATE_EVENT_FRACTION = 0.0  # >0 injects late events; the L17 watermark demo sets this to 0.1
LATE_EVENT_LAG_MINUTES = (5, 20)  # how far BACK in event time a late event is shifted

# NOTE on SPEED: this compresses event-time into wall-clock time so we can show
# in minutes what really spans days. It is a theatrical device for teaching only.
# Real producers have no such setting — events flow at the rate they actually occur.


def build_producer() -> tuple[Producer, AvroSerializer]:
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = open(SCHEMA_PATH).read()
    avro_serializer = AvroSerializer(sr_client, schema_str)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,  # no duplicate writes on retry
    })
    return producer, avro_serializer


def delivery_report(err, msg):
    if err is not None:
        ic(f"Delivery failed for key {msg.key()}: {err}")


def load_trips() -> pd.DataFrame:
    df = pd.read_parquet(PARQUET_PATH)
    # TLC data has a handful of rows with bogus timestamps (2002, 2009, ...);
    # keep only trips that actually fall in the pinned January 2024 month.
    df = df[df["tpep_pickup_datetime"].between("2024-01-01", "2024-02-01")]
    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)
    if MAX_EVENTS:
        df = df.head(MAX_EVENTS)
    return df


def to_record(row) -> dict:
    pickup_ts = row["tpep_pickup_datetime"]
    if LATE_EVENT_FRACTION and random.random() < LATE_EVENT_FRACTION:
        # Shift on a minutes scale: the lag must exceed the window size + watermark
        # of the L17 query, otherwise the event is merely out of order, not late.
        lag_minutes = random.randint(*LATE_EVENT_LAG_MINUTES)
        pickup_ts = pickup_ts - pd.Timedelta(minutes=lag_minutes)

    return {
        "pickup_ts": int(pickup_ts.timestamp() * 1000),
        "dropoff_ts": int(row["tpep_dropoff_datetime"].timestamp() * 1000),
        "pu_location_id": int(row["PULocationID"]) if not pd.isna(row["PULocationID"]) else None,
        "do_location_id": int(row["DOLocationID"]) if not pd.isna(row["DOLocationID"]) else None,
        "fare_amount": float(row["fare_amount"]),
        "tip_amount": float(row["tip_amount"]) if not pd.isna(row["tip_amount"]) else None,
        "passenger_count": int(row["passenger_count"]) if not pd.isna(row["passenger_count"]) else None,
    }


def replay_trips():
    df = load_trips()
    producer, avro_serializer = build_producer()

    first_event_time = df["tpep_pickup_datetime"].iloc[0]
    wall_clock_start = time.time()
    sent = 0
    while True:
        for _, row in df.iterrows():
            # Event-time simulation: pace publishing to a compressed wall-clock time.
            elapsed_event_seconds = (row["tpep_pickup_datetime"] - first_event_time).total_seconds()
            target_wall_time = wall_clock_start + elapsed_event_seconds / SPEED
            now = time.time()
            if target_wall_time > now:
                time.sleep(target_wall_time - now)

            record = to_record(row)
            producer.produce(
                topic=TOPIC,
                key=str(row["PULocationID"]),  # key → partition routing, ordering per zone
                value=avro_serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
            producer.poll(0)  # non-blocking: serve delivery callbacks

            sent += 1
            if sent % 100 == 0:
                ic(sent)

        producer.flush()
        ic(sent, TOPIC)
        time.sleep(5)  # pause before replaying the file again


if __name__ == "__main__":
    replay_trips()
