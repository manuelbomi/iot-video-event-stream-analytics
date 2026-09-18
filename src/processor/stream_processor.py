"""MQTT-based stream processor for per-zone occupancy events.

Responsibilities:

1. Subscribe to `venue/+/occupancy` on an MQTT broker (one topic per zone).
2. Maintain a rolling Welford mean/variance per zone.
3. Maintain a time-bucketed occupancy aggregation per zone (a simple
   "heatmap over time": average/max occupancy per fixed-size time bucket),
   useful for a dashboard to show "how busy has this zone been over the
   last hour".
4. Run Z-score anomaly detection on every incoming event, comparing it to
   the zone's rolling stats *before* folding the new value in.
5. On an anomaly, record an alert and forward it to a configurable webhook
   URL (if one is set) so downstream systems get pushed a notification
   instead of having to poll this service.

The core event-processing logic (`process_event`) is deliberately free of
any MQTT-specific code so it can be unit tested directly. The MQTT glue
(`start` / `stop` / paho-mqtt callbacks) wraps that logic for real use.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import paho.mqtt.client as mqtt

from src.processor.anomaly import AnomalyResult, ZScoreAnomalyDetector
from src.processor.welford import WelfordAccumulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [processor] %(message)s")
logger = logging.getLogger("stream_processor")

OCCUPANCY_TOPIC_FILTER = "venue/+/occupancy"


def _parse_timestamp(raw: Optional[str]) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return datetime.now(timezone.utc)


class TimeBucketAggregator:
    """Aggregates a metric into fixed-size time buckets, keeping only the
    most recent `max_buckets` buckets (a rolling "heatmap over time").
    """

    def __init__(self, bucket_seconds: int = 60, max_buckets: int = 60) -> None:
        if bucket_seconds <= 0:
            raise ValueError("bucket_seconds must be positive")
        if max_buckets <= 0:
            raise ValueError("max_buckets must be positive")
        self.bucket_seconds = bucket_seconds
        self.max_buckets = max_buckets
        self._buckets: "OrderedDict[int, dict]" = OrderedDict()

    def add(self, value: float, timestamp: Optional[datetime] = None) -> None:
        ts = timestamp or datetime.now(timezone.utc)
        bucket_key = int(ts.timestamp() // self.bucket_seconds) * self.bucket_seconds
        bucket = self._buckets.get(bucket_key)
        if bucket is None:
            bucket = {"count": 0, "sum": 0.0, "max": float("-inf")}
            self._buckets[bucket_key] = bucket
            while len(self._buckets) > self.max_buckets:
                self._buckets.popitem(last=False)
        bucket["count"] += 1
        bucket["sum"] += value
        bucket["max"] = max(bucket["max"], value)

    def snapshot(self) -> list[dict]:
        result = []
        for key, bucket in self._buckets.items():
            avg = bucket["sum"] / bucket["count"] if bucket["count"] else 0.0
            result.append(
                {
                    "bucket_start": datetime.fromtimestamp(key, tz=timezone.utc).isoformat(),
                    "avg_occupancy": round(avg, 2),
                    "max_occupancy": bucket["max"],
                    "sample_count": bucket["count"],
                }
            )
        return result


@dataclass
class Alert:
    zone: str
    timestamp: str
    occupancy: float
    z_score: float
    mean: float
    std: float
    reason: str

    def to_dict(self) -> dict:
        return {
            "zone": self.zone,
            "timestamp": self.timestamp,
            "occupancy": self.occupancy,
            "z_score": round(self.z_score, 3),
            "mean": round(self.mean, 3),
            "std": round(self.std, 3),
            "reason": self.reason,
        }


class StreamProcessor:
    """Consumes occupancy events (from MQTT or directly via `process_event`)
    and maintains rolling stats, a time-bucketed heatmap, and an anomaly
    alert feed per zone.
    """

    def __init__(
        self,
        zones: Optional[list[str]] = None,
        webhook_url: Optional[str] = None,
        anomaly_threshold: float = 3.0,
        min_samples: int = 10,
        bucket_seconds: int = 60,
        max_buckets: int = 60,
        webhook_timeout: float = 3.0,
        max_alerts: int = 200,
        on_alert: Optional[Callable[[Alert], None]] = None,
        on_event: Optional[Callable[[str, dict, AnomalyResult], None]] = None,
        http_post: Optional[Callable[..., None]] = None,
    ) -> None:
        self.webhook_url = webhook_url
        self.anomaly_threshold = anomaly_threshold
        self.min_samples = min_samples
        self.bucket_seconds = bucket_seconds
        self.max_buckets = max_buckets
        self.webhook_timeout = webhook_timeout
        self.on_alert = on_alert
        self.on_event = on_event
        # Injectable HTTP POST function, used by tests to avoid real network
        # calls. Defaults to httpx's module-level post.
        self._http_post = http_post

        self.stats: dict[str, WelfordAccumulator] = {}
        self.detectors: dict[str, ZScoreAnomalyDetector] = {}
        self.heatmaps: dict[str, TimeBucketAggregator] = {}
        self.latest: dict[str, dict] = {}
        self.alerts: deque = deque(maxlen=max_alerts)

        self._lock = threading.Lock()
        self._client: Optional[mqtt.Client] = None

        for zone in zones or []:
            self._ensure_zone(zone)

    # ------------------------------------------------------------------
    # Core processing logic (no MQTT dependency -- directly unit testable)
    # ------------------------------------------------------------------

    def _ensure_zone(self, zone: str) -> None:
        if zone not in self.stats:
            self.stats[zone] = WelfordAccumulator()
            self.detectors[zone] = ZScoreAnomalyDetector(
                threshold=self.anomaly_threshold, min_samples=self.min_samples
            )
            self.heatmaps[zone] = TimeBucketAggregator(self.bucket_seconds, self.max_buckets)

    def process_event(self, event: dict) -> AnomalyResult:
        """Process a single decoded occupancy event dict. Thread-safe.

        Expected shape (extra keys are ignored):
            {"zone": "zone-1", "occupancy": 42, "timestamp": "...", ...}
        """
        zone = str(event["zone"])
        occupancy = float(event["occupancy"])
        timestamp = _parse_timestamp(event.get("timestamp"))

        with self._lock:
            self._ensure_zone(zone)
            stats = self.stats[zone]
            detector = self.detectors[zone]

            result = detector.evaluate(occupancy, stats)
            stats.update(occupancy)
            self.heatmaps[zone].add(occupancy, timestamp)
            self.latest[zone] = {
                "zone": zone,
                "occupancy": occupancy,
                "timestamp": timestamp.isoformat(),
                "mean": stats.mean,
                "std": stats.std,
                "count": stats.count,
            }

            alert: Optional[Alert] = None
            if result.is_anomaly:
                alert = Alert(
                    zone=zone,
                    timestamp=timestamp.isoformat(),
                    occupancy=occupancy,
                    z_score=result.z_score,
                    mean=result.mean,
                    std=result.std,
                    reason=result.reason,
                )
                self.alerts.append(alert)

        if self.on_event is not None:
            self.on_event(zone, event, result)

        if alert is not None:
            logger.warning(
                "ANOMALY zone=%s occupancy=%.1f z=%.2f mean=%.1f std=%.2f",
                zone,
                occupancy,
                result.z_score,
                result.mean,
                result.std,
            )
            self._dispatch_alert(alert)
            if self.on_alert is not None:
                self.on_alert(alert)

        return result

    def _dispatch_alert(self, alert: Alert) -> None:
        if not self.webhook_url:
            return
        payload = alert.to_dict()
        try:
            post = self._http_post or self._default_http_post
            post(self.webhook_url, json=payload, timeout=self.webhook_timeout)
        except Exception as exc:  # noqa: BLE001 - alert delivery must never crash the processor
            logger.error("Failed to deliver alert to webhook %s: %s", self.webhook_url, exc)

    @staticmethod
    def _default_http_post(url: str, json: dict, timeout: float) -> None:  # noqa: A002
        import httpx

        response = httpx.post(url, json=json, timeout=timeout)
        response.raise_for_status()

    def get_snapshot(self) -> dict:
        """A JSON-serializable snapshot suitable for a dashboard."""
        with self._lock:
            zones_snapshot = {}
            for zone, stats in self.stats.items():
                zones_snapshot[zone] = {
                    "latest": self.latest.get(zone),
                    "stats": {
                        "count": stats.count,
                        "mean": round(stats.mean, 3),
                        "std": round(stats.std, 3),
                        "min": stats.min_value if stats.count else None,
                        "max": stats.max_value if stats.count else None,
                    },
                    "heatmap": self.heatmaps[zone].snapshot(),
                }
            alerts_snapshot = [a.to_dict() for a in self.alerts]
        return {"zones": zones_snapshot, "alerts": alerts_snapshot}

    # ------------------------------------------------------------------
    # MQTT glue
    # ------------------------------------------------------------------

    def _on_connect(self, client: mqtt.Client, userdata, flags, rc) -> None:  # noqa: ANN001
        if rc == 0:
            logger.info("Connected to MQTT broker, subscribing to %s", OCCUPANCY_TOPIC_FILTER)
            client.subscribe(OCCUPANCY_TOPIC_FILTER, qos=1)
        else:
            logger.error("Failed to connect to MQTT broker, rc=%s", rc)

    def _on_message(self, client: mqtt.Client, userdata, msg) -> None:  # noqa: ANN001
        try:
            event = json.loads(msg.payload.decode("utf-8"))
            self.process_event(event)
        except Exception as exc:  # noqa: BLE001 - never let a bad message kill the loop
            logger.error("Failed to process message on %s: %s", msg.topic, exc)

    def start(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "stream-processor",
        blocking: bool = False,
    ) -> None:
        client = mqtt.Client(client_id=client_id)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.connect(host, port, keepalive=30)
        self._client = client
        if blocking:
            client.loop_forever()
        else:
            client.loop_start()

    def stop(self) -> None:
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    parser.add_argument(
        "--webhook-url", default=os.environ.get("ALERT_WEBHOOK_URL"), help="Webhook URL for alerts"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=float(os.environ.get("ANOMALY_THRESHOLD", "3.0")),
    )
    parser.add_argument(
        "--min-samples", type=int, default=int(os.environ.get("ANOMALY_MIN_SAMPLES", "10"))
    )
    parser.add_argument(
        "--bucket-seconds", type=int, default=int(os.environ.get("BUCKET_SECONDS", "60"))
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    processor = StreamProcessor(
        webhook_url=args.webhook_url,
        anomaly_threshold=args.threshold,
        min_samples=args.min_samples,
        bucket_seconds=args.bucket_seconds,
    )
    logger.info(
        "Starting stream processor (broker=%s:%s, webhook=%s, threshold=%.1f)",
        args.host,
        args.port,
        args.webhook_url,
        args.threshold,
    )
    processor.start(host=args.host, port=args.port, blocking=True)


if __name__ == "__main__":
    main()
