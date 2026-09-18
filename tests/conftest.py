"""Shared pytest fixtures/helpers."""

from __future__ import annotations

import os
import socket


def tcp_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Best-effort check for whether something is listening on host:port.

    Used to decide whether broker-dependent integration tests should run
    or skip cleanly. This is intentionally simple (just a TCP connect) --
    good enough to distinguish "no broker running here" (typical local dev
    machine) from "a broker is up" (CI, or a developer running
    docker compose locally).
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


MQTT_TEST_HOST = os.environ.get("MQTT_TEST_HOST", os.environ.get("MQTT_HOST", "localhost"))
MQTT_TEST_PORT = int(os.environ.get("MQTT_TEST_PORT", os.environ.get("MQTT_PORT", "1883")))

KAFKA_TEST_BOOTSTRAP = os.environ.get(
    "KAFKA_TEST_BOOTSTRAP_SERVERS", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
)
