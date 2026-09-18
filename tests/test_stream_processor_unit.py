"""Unit tests for StreamProcessor's core logic -- no MQTT broker required.

These exercise `process_event` directly (the transport-agnostic path),
the time-bucket heatmap aggregator, and webhook alert dispatch using an
injected HTTP POST function so no real network call is made.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.processor.stream_processor import Alert, StreamProcessor, TimeBucketAggregator


def make_event(zone: str, occupancy: float, ts: datetime | None = None) -> dict:
    ts = ts or datetime.now(timezone.utc)
    return {"zone": zone, "occupancy": occupancy, "timestamp": ts.isoformat()}


def test_process_event_builds_rolling_stats_per_zone() -> None:
    processor = StreamProcessor(min_samples=5)
    for occ in [20, 21, 19, 22, 20, 21]:
        processor.process_event(make_event("zone-1", occ))

    snapshot = processor.get_snapshot()
    assert "zone-1" in snapshot["zones"]
    z1 = snapshot["zones"]["zone-1"]
    assert z1["stats"]["count"] == 6
    assert 19 <= z1["stats"]["mean"] <= 22
    assert z1["latest"]["occupancy"] == 21


def test_process_event_flags_spike_and_creates_alert() -> None:
    alerts_seen = []
    processor = StreamProcessor(
        min_samples=5, anomaly_threshold=3.0, on_alert=lambda alert: alerts_seen.append(alert)
    )

    for occ in [20, 21, 19, 22, 20, 21, 20, 19]:
        processor.process_event(make_event("zone-2", occ))

    result = processor.process_event(make_event("zone-2", 500))
    assert result.is_anomaly is True
    assert len(alerts_seen) == 1
    assert isinstance(alerts_seen[0], Alert)
    assert alerts_seen[0].zone == "zone-2"

    snapshot = processor.get_snapshot()
    assert len(snapshot["alerts"]) == 1
    assert snapshot["alerts"][0]["zone"] == "zone-2"


def test_zones_are_independent() -> None:
    processor = StreamProcessor(min_samples=3)
    for occ in [10, 11, 10, 12]:
        processor.process_event(make_event("zone-a", occ))
    for occ in [90, 91, 89, 92]:
        processor.process_event(make_event("zone-b", occ))

    snapshot = processor.get_snapshot()
    assert snapshot["zones"]["zone-a"]["stats"]["mean"] < 20
    assert snapshot["zones"]["zone-b"]["stats"]["mean"] > 80


def test_webhook_dispatch_called_with_expected_payload() -> None:
    captured = {}

    def fake_post(url, json, timeout):  # noqa: A002
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout

    processor = StreamProcessor(
        min_samples=5,
        anomaly_threshold=3.0,
        webhook_url="http://example.invalid/alerts",
        http_post=fake_post,
    )
    for occ in [20, 21, 19, 22, 20, 21, 20, 19]:
        processor.process_event(make_event("zone-3", occ))
    processor.process_event(make_event("zone-3", 500))

    assert captured["url"] == "http://example.invalid/alerts"
    assert captured["json"]["zone"] == "zone-3"
    assert captured["json"]["occupancy"] == 500
    assert captured["json"]["z_score"] > 3.0


def test_webhook_failure_does_not_raise() -> None:
    def failing_post(url, json, timeout):  # noqa: A002
        raise ConnectionError("simulated network failure")

    processor = StreamProcessor(
        min_samples=5,
        anomaly_threshold=3.0,
        webhook_url="http://example.invalid/alerts",
        http_post=failing_post,
    )
    for occ in [20, 21, 19, 22, 20, 21, 20, 19]:
        processor.process_event(make_event("zone-4", occ))

    # Should not raise even though the webhook call fails internally.
    result = processor.process_event(make_event("zone-4", 500))
    assert result.is_anomaly is True


def test_time_bucket_aggregator_groups_by_bucket() -> None:
    agg = TimeBucketAggregator(bucket_seconds=60, max_buckets=10)
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    agg.add(10.0, base)
    agg.add(20.0, base + timedelta(seconds=10))
    agg.add(100.0, base + timedelta(seconds=70))  # next bucket

    snapshot = agg.snapshot()
    assert len(snapshot) == 2
    first, second = snapshot
    assert first["sample_count"] == 2
    assert first["avg_occupancy"] == 15.0
    assert second["sample_count"] == 1
    assert second["avg_occupancy"] == 100.0


def test_time_bucket_aggregator_respects_max_buckets() -> None:
    agg = TimeBucketAggregator(bucket_seconds=1, max_buckets=3)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(10):
        agg.add(float(i), base + timedelta(seconds=i))

    snapshot = agg.snapshot()
    assert len(snapshot) == 3
