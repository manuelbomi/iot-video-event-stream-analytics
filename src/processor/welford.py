"""Welford's online algorithm for streaming mean and variance.

Why this exists: when you're ingesting a live stream of events (occupancy
counts, entry/exit deltas, etc.) you don't want to keep every observation
around just to compute a mean and variance. Welford's algorithm updates a
running mean and running sum-of-squared-deviations in O(1) time and O(1)
memory per update, and it is numerically stable (it does not compute
sum(x) and sum(x^2) separately, which can lose precision through
catastrophic cancellation for large streams).

Reference: B. P. Welford, "Note on a Method for Calculating Corrected Sums
of Squares and Products", Technometrics, 1962.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class WelfordStats:
    """Immutable-ish snapshot of the current rolling statistics."""

    count: int
    mean: float
    variance: float
    std: float
    min_value: float
    max_value: float


class WelfordAccumulator:
    """Streaming mean / variance accumulator.

    Usage:
        acc = WelfordAccumulator()
        for x in stream:
            acc.update(x)
            stats = acc.stats()

    The accumulator keeps only a handful of scalars regardless of how many
    values it has seen, which is what makes it suitable for long-running,
    per-zone rolling statistics in a stream processor.
    """

    __slots__ = ("count", "mean", "_m2", "min_value", "max_value")

    def __init__(self) -> None:
        self.count: int = 0
        self.mean: float = 0.0
        # _m2 is the running sum of squared differences from the current mean.
        # variance = _m2 / count (population) or _m2 / (count - 1) (sample).
        self._m2: float = 0.0
        self.min_value: float = float("inf")
        self.max_value: float = float("-inf")

    def update(self, value: float) -> None:
        """Feed a single new observation into the running statistics."""
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self._m2 += delta * delta2

        if value < self.min_value:
            self.min_value = value
        if value > self.max_value:
            self.max_value = value

    @property
    def variance(self) -> float:
        """Sample variance (Bessel-corrected, ddof=1).

        Returns 0.0 when fewer than 2 samples have been observed, since
        sample variance is undefined with a single point.
        """
        if self.count < 2:
            return 0.0
        return self._m2 / (self.count - 1)

    @property
    def population_variance(self) -> float:
        """Population variance (ddof=0), matching numpy's default `.var()`."""
        if self.count < 1:
            return 0.0
        return self._m2 / self.count

    @property
    def std(self) -> float:
        return self.variance ** 0.5

    @property
    def population_std(self) -> float:
        return self.population_variance ** 0.5

    def stats(self) -> WelfordStats:
        return WelfordStats(
            count=self.count,
            mean=self.mean,
            variance=self.variance,
            std=self.std,
            min_value=self.min_value if self.count else 0.0,
            max_value=self.max_value if self.count else 0.0,
        )

    def reset(self) -> None:
        self.__init__()  # type: ignore[misc]
