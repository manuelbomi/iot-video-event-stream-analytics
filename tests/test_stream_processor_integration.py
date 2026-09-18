"""End-to-end integration test: real Mosquitto broker -> StreamProcessor.

This test publishes a handful of synthetic occupancy events (including
one deliberate anomaly) to a real MQTT broker and asserts the
`StreamProcessor` consumes them and emits the expected alert.

It needs an actual MQTT broker reachable at MQTT_TEST_HOST:MQTT_TEST_PORT
(defaults to localhost:1883). If none is reachable, the test is skipped
cleanly -- most local dev machines won't have Mosquitto running. In CI,
the workflow spins up an `eclipse-mosquitto` service container, so this
test runs (and must pass) there.

To run it locally:

    docker compose up -d mosquitto
    pytest tests/test_stream_processor_integration.py -v
"""

from __future__ import annotations

import json
import threading
import time

import paho.mqtt.client as mqtt
import pytest

from src.processor.stream_processor import StreamProcessor
from tests.conftest import MQTT_TEST_HOST, MQTT_TEST_PORT, tcp_port_open

pytestmark = pytest.mark.skipif(
    not tcp_port_open(MQTT_TEST_HOST, MQTT_TEST_PORT),
    reason=f"No MQTT broker reachable at {MQTT_TEST_HOST}:{MQTT_TEST_PORT}",
)


def publish_events(host: str, port: int, zone: str, occupancies: list[float]) -> None:
    client = mqtt.Client(client_id=f"test-publisher-{zone}-{time.time_ns()}")
    client.connect(host, port, keepalive=10)
    client.loop_start()
    try:
        for occ in occupancies:
            payload = json.dumps({"zone": zone, "occupancy": occ})
            client.publish(f"venue/{zone}/occupancy", payload, qos=1)
            time.sleep(0.05)
        # Give the broker a moment to flush the last publishes before we
        # disconnect the publisher.
        time.sleep(0.3)
    finally:
        client.loop_stop()
        client.disconnect()


def test_processor_consumes_events_and_flags_injected_spike() -> None:
    zone = f"zone-integration-{int(time.time())}"
    alerts_seen: list = []
    alert_event = threading.Event()

    def on_alert(alert) -> None:
        alerts_seen.append(alert)
        alert_event.set()

    processor = StreamProcessor(min_samples=5, anomaly_threshold=3.0, on_alert=on_alert)
    processor.start(host=MQTT_TEST_HOST, port=MQTT_TEST_PORT, client_id="test-processor", blocking=False)

    try:
        # Let the subscriber finish connecting/subscribing before publishing.
        time.sleep(1.0)

        normal_occupancies = [20, 21, 19, 22, 20, 21, 20, 19, 21, 20]
        publish_events(MQTT_TEST_HOST, MQTT_TEST_PORT, zone, normal_occupancies)

        # No anomaly should have fired yet from purely normal traffic.
        assert not alert_event.is_set()

        # Now publish the injected anomaly.
        publish_events(MQTT_TEST_HOST, MQTT_TEST_PORT, zone, [500])

        fired = alert_event.wait(timeout=10.0)
        assert fired, "Expected an anomaly alert to be emitted for the injected spike"
        assert len(alerts_seen) == 1
        assert alerts_seen[0].zone == zone
        assert alerts_seen[0].occupancy == 500

        snapshot = processor.get_snapshot()
        assert snapshot["zones"][zone]["stats"]["count"] == len(normal_occupancies) + 1
        assert snapshot["alerts"][-1]["zone"] == zone
    finally:
        processor.stop()
