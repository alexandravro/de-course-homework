# Kafka consumer for the taxi-trips topic.
#
# Deserializes Avro messages through Schema Registry and prints partition +
# offset for every event, so the live demo can show offset tracking and
# consumer-group rebalancing.
#
# Run two of these in DIFFERENT terminals — both share GROUP_ID below, so
# Kafka splits partitions between them live:
#
#   uv run consumer.py
#   uv run consumer.py     # second terminal, same script, same GROUP_ID
#
# To consume the full topic independently (own offsets), edit GROUP_ID below.

import time

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from icecream import ic

TOPIC = "taxi-trips"
GROUP_ID = "taxi-consumer-demo"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


def on_assign(consumer, partitions):
    parts = sorted(p.partition for p in partitions)
    ic(f">>> Rebalance: this consumer now owns partitions {parts}")


def on_revoke(consumer, partitions):
    parts = sorted(p.partition for p in partitions)
    ic(f">>> Rebalance: partitions {parts} revoked from this consumer")


def run_consumer():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "SOMETHING",
        "auto.offset.reset": "earliest",  # read from the start of the topic
        "enable.auto.commit": True,  # commit offsets to __consumer_offsets
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    ic(f"Consuming topic '{TOPIC}' as group '{GROUP_ID}'. Ctrl-C to stop.")

    seen = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                ic(msg.error())
                continue

            event = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            seen += 1
            ic(msg.partition(), msg.offset(), event["pu_location_id"], event["fare_amount"])
            time.sleep(5)  # slow down for demo purposes
    except KeyboardInterrupt:
        ic(seen)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
