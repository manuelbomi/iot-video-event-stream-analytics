"""Tests for the Kafka consumer variant.

The MQTT path is this repo's fully-tested end-to-end path (see
`test_stream_processor_integration.py`), since a Mosquitto broker is
trivial to run in Docker/CI. Kafka is heavier to stand up, so this test
is skipped automatically whenever no Kafka broker is reachable --
which will be the case on most local dev machines and in this repo's
default CI job (no Kafka service container is configured there).

To actually exercise this path locally:

    docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0
    KAFKA_TEST_BOOTSTRAP_SERVERS=localhost:9092 pytest tests/test_kafka_consumer.py -v
"""

from __future__ import annotations

import json
import time

import pytest

from src.processor.kafka_consumer import DEFAULT_TOPIC, KafkaStreamConsumer
from src.processor.stream_processor import StreamProcessor
from tests.conftest import KAFKA_TEST_BOOTSTRAP


def _kafka_available() -> bool:
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_TEST_BOOTSTRAP.split(","),
            request_timeout_ms=2000,
            api_version_auto_timeout_ms=2000,
        )
        producer.close(timeout=2)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _kafka_available(),
    reason=f"No Kafka broker reachable at {KAFKA_TEST_BOOTSTRAP}",
)


def test_kafka_consumer_processes_published_events() -> None:
    from kafka import KafkaProducer

    zone = f"zone-kafka-{int(time.time())}"
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_TEST_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    events = [{"zone": zone, "occupancy": occ} for occ in [20, 21, 19, 22, 20]]
    for event in events:
        producer.send(DEFAULT_TOPIC, value=event, key=zone.encode("utf-8"))
    producer.flush()
    producer.close()

    processor = StreamProcessor(min_samples=3)
    consumer = KafkaStreamConsumer(
        processor,
        bootstrap_servers=KAFKA_TEST_BOOTSTRAP,
        group_id=f"test-group-{zone}",
    )
    processed = consumer.run(max_messages=len(events))

    assert processed == len(events)
    snapshot = processor.get_snapshot()
    assert snapshot["zones"][zone]["stats"]["count"] == len(events)
