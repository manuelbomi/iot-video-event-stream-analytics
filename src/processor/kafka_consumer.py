"""Kafka-based variant of the stream processor.

The MQTT path (`stream_processor.py`) is the primary, fully-tested path in
this repo -- it's lightweight and trivial to stand up in Docker/CI with a
single `eclipse-mosquitto` container. This module is a documented
*alternative* consumer for when you outgrow MQTT:

- Very high event volume (many thousands of events/sec across many zones/
  cameras) where a broker built for pub/sub fan-out to a handful of
  subscribers starts to strain.
- You need replay: the ability to re-process the last N hours of events
  (e.g. to backfill a new anomaly model, or reprocess after a bug fix)
  without the producer re-sending anything. Kafka retains events on disk
  for a configurable retention period; MQTT does not.
- You need multiple independent consumer groups reading the *same* stream
  for different purposes (e.g. one consumer group doing anomaly detection,
  another archiving raw events to cold storage, another feeding a
  real-time ML feature store) without the producer knowing or caring how
  many consumers exist.

This module re-uses the exact same event-processing logic
(`StreamProcessor.process_event`) as the MQTT consumer -- only the
transport differs. That's the point: rolling stats, anomaly detection, and
alert dispatch are transport-agnostic.

Requires a running Kafka broker. Tests for this module are skipped
automatically when no broker is reachable (see tests/test_kafka_consumer.py).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Optional

from src.processor.stream_processor import StreamProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [kafka-processor] %(message)s")
logger = logging.getLogger("kafka_consumer")

DEFAULT_TOPIC = "venue.occupancy"


def probe_kafka(bootstrap_servers: str, timeout: float = 2.0) -> bool:
    """Best-effort check for whether a Kafka broker is reachable.

    Used by tests (and optionally by operators) to decide whether it's
    worth attempting a real connection, so we fail fast/skip cleanly
    instead of hanging on a connection timeout.
    """
    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","),
            api_version_auto_timeout_ms=int(timeout * 1000),
            request_timeout_ms=int(timeout * 1000),
        )
        consumer.close()
        return True
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False


class KafkaStreamConsumer:
    """Consumes occupancy events from a Kafka topic and feeds them into a
    `StreamProcessor` for rolling stats + anomaly detection, exactly like
    the MQTT consumer does.

    Expected message value: JSON-encoded event dict, same schema as the
    MQTT producer emits, e.g.:

        {"zone": "zone-1", "occupancy": 42, "timestamp": "...", ...}

    The zone can also be carried in the Kafka message key, which is how
    the accompanying producer-side code would typically partition events
    so that all events for a given zone land on the same partition
    (preserving per-zone ordering).
    """

    def __init__(
        self,
        processor: StreamProcessor,
        bootstrap_servers: str = "localhost:9092",
        topic: str = DEFAULT_TOPIC,
        group_id: str = "stream-processor",
    ) -> None:
        self.processor = processor
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._consumer = None

    def run(self, max_messages: Optional[int] = None) -> int:
        """Consume messages and feed them to the processor.

        Returns the number of messages processed. `max_messages` is mainly
        useful for tests/demos so the loop terminates instead of blocking
        forever waiting for new events.
        """
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            group_id=self.group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            consumer_timeout_ms=10_000,
        )
        self._consumer = consumer
        processed = 0
        try:
            for message in consumer:
                try:
                    self.processor.process_event(message.value)
                    processed += 1
                except Exception as exc:  # noqa: BLE001
                    logger.error("Failed to process Kafka message: %s", exc)
                if max_messages is not None and processed >= max_messages:
                    break
        finally:
            consumer.close()
        return processed


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--group-id", default=os.environ.get("KAFKA_GROUP_ID", "stream-processor"))
    parser.add_argument("--webhook-url", default=os.environ.get("ALERT_WEBHOOK_URL"))
    parser.add_argument("--threshold", type=float, default=float(os.environ.get("ANOMALY_THRESHOLD", "3.0")))
    parser.add_argument("--min-samples", type=int, default=int(os.environ.get("ANOMALY_MIN_SAMPLES", "10")))
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    processor = StreamProcessor(
        webhook_url=args.webhook_url,
        anomaly_threshold=args.threshold,
        min_samples=args.min_samples,
    )
    consumer = KafkaStreamConsumer(
        processor,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
    )
    logger.info(
        "Starting Kafka stream consumer (servers=%s, topic=%s, group=%s)",
        args.bootstrap_servers,
        args.topic,
        args.group_id,
    )
    consumer.run()


if __name__ == "__main__":
    main()
