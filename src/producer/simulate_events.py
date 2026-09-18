"""Synthetic vision-analytics event producer.

Simulates what a fleet of AI-vision cameras would emit for a set of
monitored zones: a periodic occupancy count per zone, plus zone entry/exit
counters. Publishes JSON events over MQTT, one topic per zone, e.g.

    venue/zone-1/occupancy
    venue/zone-2/occupancy
    venue/zone-3/occupancy

This is meant to stand in for the real "camera + AI vision model" pipeline
so the rest of the stack (stream processor, anomaly detection, dashboard)
can be built and tested without needing real cameras or footage.

Run standalone:

    python -m src.producer.simulate_events --rate 2 --duration 120 \\
        --spike-zone zone-2 --spike-at 30 --spike-magnitude 8

Every zone wanders around its own baseline occupancy using a simple mean
reverting random walk, so the numbers look plausible rather than pure
white noise. An optional "spike" can be injected into one zone at a
specific point in time to simulate a sudden crowd surge, for demoing the
anomaly detector end-to-end.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
logger = logging.getLogger("producer")

DEFAULT_ZONES = ["zone-1", "zone-2", "zone-3"]
TOPIC_TEMPLATE = "venue/{zone}/occupancy"


@dataclass
class ZoneState:
    """Tracks the simulated occupancy for a single zone."""

    name: str
    baseline: float
    occupancy: float
    volatility: float = 1.5
    total_entries: int = 0
    total_exits: int = 0
    rng: random.Random = field(default_factory=random.Random)

    def step(self) -> tuple[int, int]:
        """Advance the zone's occupancy by one tick using a mean-reverting
        random walk, and return (entries, exits) for this tick.
        """
        reversion = (self.baseline - self.occupancy) * 0.1
        noise = self.rng.gauss(0, self.volatility)
        self.occupancy = max(0.0, self.occupancy + reversion + noise)

        # Derive plausible entry/exit counts from the change in occupancy.
        delta = self.rng.randint(0, 3)
        entries = delta
        exits = max(0, delta - self.rng.randint(-1, 1))
        self.total_entries += entries
        self.total_exits += exits
        return entries, exits

    def inject_spike(self, magnitude: float) -> None:
        """Simulate a sudden crowd surge (e.g. a gate opening, an incident,
        an unexpected influx) by jumping occupancy up by `magnitude` times
        the zone's normal volatility.
        """
        self.occupancy += magnitude * max(self.volatility, 1.0)
        logger.warning(
            "Injecting artificial occupancy spike into %s (+%.1f) -> occupancy=%.1f",
            self.name,
            magnitude * max(self.volatility, 1.0),
            self.occupancy,
        )


def build_event(zone: ZoneState, entries: int, exits: int) -> dict:
    return {
        "zone": zone.name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "occupancy": round(zone.occupancy),
        "entries": entries,
        "exits": exits,
        "event_type": "occupancy_update",
    }


def make_zone_states(zone_names: list[str], seed: Optional[int] = None) -> list[ZoneState]:
    rng = random.Random(seed)
    zones = []
    for name in zone_names:
        baseline = rng.uniform(15, 60)
        zones.append(
            ZoneState(
                name=name,
                baseline=baseline,
                occupancy=baseline,
                volatility=rng.uniform(1.0, 3.0),
                rng=random.Random(rng.random()),
            )
        )
    return zones


def run(
    broker_host: str,
    broker_port: int,
    zones: list[str],
    rate_hz: float,
    duration: Optional[float],
    spike_zone: Optional[str],
    spike_at: Optional[float],
    spike_magnitude: float,
    seed: Optional[int],
    client_id: str = "vision-event-simulator",
) -> None:
    client = mqtt.Client(client_id=client_id)
    logger.info("Connecting to MQTT broker at %s:%s", broker_host, broker_port)
    client.connect(broker_host, broker_port, keepalive=30)
    client.loop_start()

    zone_states = make_zone_states(zones, seed=seed)
    period = 1.0 / rate_hz if rate_hz > 0 else 1.0
    start = time.monotonic()
    spike_fired = spike_zone is None or spike_at is None

    try:
        tick = 0
        while True:
            elapsed = time.monotonic() - start
            if duration is not None and elapsed >= duration:
                logger.info("Duration %.1fs reached, stopping.", duration)
                break

            if not spike_fired and spike_at is not None and elapsed >= spike_at:
                target = next((z for z in zone_states if z.name == spike_zone), None)
                if target is not None:
                    target.inject_spike(spike_magnitude)
                spike_fired = True

            for zone in zone_states:
                entries, exits = zone.step()
                event = build_event(zone, entries, exits)
                topic = TOPIC_TEMPLATE.format(zone=zone.name)
                payload = json.dumps(event)
                client.publish(topic, payload, qos=1)
                logger.info("-> %s: %s", topic, payload)

            tick += 1
            time.sleep(period)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    finally:
        client.loop_stop()
        client.disconnect()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--zones",
        default=",".join(DEFAULT_ZONES),
        help="Comma-separated list of zone names to simulate",
    )
    parser.add_argument(
        "--rate", type=float, default=1.0, help="Publish rate in Hz (events per zone per second)"
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Total run duration in seconds (default: run forever)",
    )
    parser.add_argument(
        "--spike-zone",
        default=None,
        help="Zone name to inject an artificial occupancy spike into (for demo/testing)",
    )
    parser.add_argument(
        "--spike-at",
        type=float,
        default=None,
        help="Seconds after start at which to inject the spike",
    )
    parser.add_argument(
        "--spike-magnitude",
        type=float,
        default=6.0,
        help="Spike size as a multiple of the zone's normal volatility",
    )
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    zone_names = [z.strip() for z in args.zones.split(",") if z.strip()]
    run(
        broker_host=args.host,
        broker_port=args.port,
        zones=zone_names,
        rate_hz=args.rate,
        duration=args.duration,
        spike_zone=args.spike_zone,
        spike_at=args.spike_at,
        spike_magnitude=args.spike_magnitude,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
