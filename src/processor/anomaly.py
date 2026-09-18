"""Z-score based anomaly detection on top of streaming Welford statistics.

The idea is simple: given the rolling mean and standard deviation of a
per-zone metric (e.g. occupancy count), an incoming value's Z-score tells
you how many standard deviations away from "normal" it is:

    z = (x - mean) / std

A value with |z| above some threshold (commonly 3.0) is treated as
statistically unusual -- i.e. a candidate anomaly worth alerting on.

Cold start handling: with very few samples, the rolling mean/std is not a
trustworthy estimate of "normal" yet (e.g. with 1 sample, std is 0 and
every subsequent value looks like an infinite-sigma outlier). We therefore
require a configurable minimum sample count before we ever flag anything,
and we also guard against a zero/near-zero standard deviation so we don't
divide by (approximately) zero on a run of identical values.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from src.processor.welford import WelfordAccumulator


@dataclass
class AnomalyResult:
    value: float
    mean: float
    std: float
    z_score: float
    is_anomaly: bool
    reason: str


class ZScoreAnomalyDetector:
    """Flags a new value as anomalous based on its Z-score against the
    rolling statistics observed *before* this value is folded in.

    Parameters
    ----------
    threshold:
        Absolute Z-score above which a value is flagged. Default 3.0
        (the common "three-sigma" rule of thumb).
    min_samples:
        Minimum number of prior observations required before the detector
        will flag anything. Below this, `is_anomaly` is always False
        (cold-start protection).
    min_std:
        Floor applied to the standard deviation before dividing, to avoid
        blowing up on a run of perfectly constant values.
    """

    def __init__(
        self,
        threshold: float = 3.0,
        min_samples: int = 10,
        min_std: float = 1e-6,
    ) -> None:
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        if min_samples < 2:
            raise ValueError("min_samples must be >= 2 (std needs >=2 points)")
        self.threshold = threshold
        self.min_samples = min_samples
        self.min_std = min_std

    def evaluate(self, value: float, stats: WelfordAccumulator) -> AnomalyResult:
        """Evaluate `value` against the accumulator's *current* state.

        Call this BEFORE `stats.update(value)` so the value being judged is
        compared against the history that came before it, not against
        itself.
        """
        if stats.count < self.min_samples:
            return AnomalyResult(
                value=value,
                mean=stats.mean,
                std=stats.std,
                z_score=0.0,
                is_anomaly=False,
                reason=f"cold_start ({stats.count}/{self.min_samples} samples)",
            )

        std = max(stats.std, self.min_std)
        z = (value - stats.mean) / std
        is_anomaly = abs(z) > self.threshold
        reason = (
            f"|z|={abs(z):.2f} > threshold={self.threshold}"
            if is_anomaly
            else f"|z|={abs(z):.2f} within threshold={self.threshold}"
        )
        return AnomalyResult(
            value=value,
            mean=stats.mean,
            std=std,
            z_score=z,
            is_anomaly=is_anomaly,
            reason=reason,
        )

    @staticmethod
    def z_score(value: float, mean: float, std: float, min_std: float = 1e-6) -> float:
        """Pure helper for computing a Z-score, useful for docs/examples/tests."""
        safe_std = max(std, min_std)
        z = (value - mean) / safe_std
        return z if math.isfinite(z) else 0.0
