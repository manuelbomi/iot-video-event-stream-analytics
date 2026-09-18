"""Tests for Z-score anomaly detection built on Welford rolling stats."""

from __future__ import annotations

import random

from src.processor.anomaly import ZScoreAnomalyDetector
from src.processor.welford import WelfordAccumulator


def test_cold_start_never_flags_before_min_samples() -> None:
    detector = ZScoreAnomalyDetector(threshold=3.0, min_samples=10)
    stats = WelfordAccumulator()

    # Feed wildly different values while below min_samples -- none should
    # be flagged, because we don't yet trust the rolling mean/std.
    values = [1.0, 1000.0, -500.0, 3.0, 800.0, 2.0, 900.0, 1.0]
    for v in values:
        result = detector.evaluate(v, stats)
        assert result.is_anomaly is False
        assert "cold_start" in result.reason
        stats.update(v)

    assert stats.count == len(values) < detector.min_samples


def test_injected_spike_is_flagged_and_only_the_spike() -> None:
    rng = random.Random(7)
    detector = ZScoreAnomalyDetector(threshold=3.0, min_samples=10)
    stats = WelfordAccumulator()

    normal_values = [50 + rng.uniform(-2.0, 2.0) for _ in range(40)]
    spike_index = 30
    sequence = list(normal_values)
    sequence[spike_index] = 200.0  # a clear, large spike far outside normal range

    flagged_indices = []
    for i, v in enumerate(sequence):
        result = detector.evaluate(v, stats)
        if result.is_anomaly:
            flagged_indices.append(i)
        stats.update(v)

    assert spike_index in flagged_indices
    # Only the spike should be flagged -- everything else is within a few
    # units of the 50.0 baseline with low volatility.
    assert flagged_indices == [spike_index]


def test_normal_values_within_threshold_are_not_flagged() -> None:
    rng = random.Random(123)
    detector = ZScoreAnomalyDetector(threshold=3.0, min_samples=10)
    stats = WelfordAccumulator()

    flagged = 0
    for _ in range(200):
        v = rng.gauss(30.0, 4.0)
        result = detector.evaluate(v, stats)
        if result.is_anomaly:
            flagged += 1
        stats.update(v)

    # With a proper gaussian and a 3-sigma threshold, false positives should
    # be rare. We allow a small amount of slack since this is still a
    # finite random sample, but this must stay a small fraction of 200.
    assert flagged <= 5


def test_zero_variance_stream_does_not_explode() -> None:
    """A run of identical values gives std=0. The detector must not divide
    by zero / produce inf or NaN -- the min_std floor should keep this sane.
    """
    detector = ZScoreAnomalyDetector(threshold=3.0, min_samples=5)
    stats = WelfordAccumulator()

    for _ in range(10):
        result = detector.evaluate(10.0, stats)
        stats.update(10.0)

    # A brand new value equal to the constant baseline should not be flagged.
    result = detector.evaluate(10.0, stats)
    assert result.is_anomaly is False
    assert result.z_score == 0.0

    # A modestly different value against a near-zero std will have a huge
    # |z|, which is mathematically correct (the stream genuinely never
    # varies), so it *should* be flagged -- and the score must be finite.
    result = detector.evaluate(11.0, stats)
    assert result.is_anomaly is True
    assert result.z_score == result.z_score  # not NaN
    assert abs(result.z_score) < float("inf")


def test_threshold_and_min_samples_validation() -> None:
    import pytest

    with pytest.raises(ValueError):
        ZScoreAnomalyDetector(threshold=0)
    with pytest.raises(ValueError):
        ZScoreAnomalyDetector(min_samples=1)


def test_static_z_score_helper_matches_manual_calculation() -> None:
    # mean=50, std=5, value=65 -> z = (65-50)/5 = 3.0
    z = ZScoreAnomalyDetector.z_score(65.0, mean=50.0, std=5.0)
    assert z == 3.0
