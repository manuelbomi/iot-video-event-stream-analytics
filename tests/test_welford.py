"""Validate WelfordAccumulator against numpy's batch mean/variance.

Welford's algorithm is only useful if it actually agrees with the
textbook batch formulas. These tests feed the same random sequences
through both the streaming accumulator and numpy's batch `.mean()` /
`.var()`, and assert they agree within a small tolerance.
"""

from __future__ import annotations

import numpy as np
import pytest

from src.processor.welford import WelfordAccumulator

SEEDS = [0, 1, 42, 12345, 7]


@pytest.mark.parametrize("seed", SEEDS)
def test_running_mean_matches_numpy(seed: int) -> None:
    rng = np.random.default_rng(seed)
    data = rng.normal(loc=50.0, scale=8.0, size=500)

    acc = WelfordAccumulator()
    for x in data:
        acc.update(float(x))

    assert acc.mean == pytest.approx(np.mean(data), rel=1e-9, abs=1e-9)


@pytest.mark.parametrize("seed", SEEDS)
def test_population_variance_matches_numpy(seed: int) -> None:
    rng = np.random.default_rng(seed)
    data = rng.normal(loc=20.0, scale=3.0, size=500)

    acc = WelfordAccumulator()
    for x in data:
        acc.update(float(x))

    # numpy's default var() (ddof=0) is population variance.
    assert acc.population_variance == pytest.approx(np.var(data), rel=1e-9, abs=1e-9)


@pytest.mark.parametrize("seed", SEEDS)
def test_sample_variance_matches_numpy_ddof1(seed: int) -> None:
    rng = np.random.default_rng(seed)
    data = rng.normal(loc=0.0, scale=1.0, size=1000)

    acc = WelfordAccumulator()
    for x in data:
        acc.update(float(x))

    assert acc.variance == pytest.approx(np.var(data, ddof=1), rel=1e-9, abs=1e-9)


def test_incremental_agreement_at_every_step() -> None:
    """Not just the final value -- the running stats should match numpy's
    batch computation over the prefix at *every* step along the way.
    """
    rng = np.random.default_rng(99)
    data = rng.normal(loc=10.0, scale=2.5, size=200)

    acc = WelfordAccumulator()
    for i, x in enumerate(data, start=1):
        acc.update(float(x))
        prefix = data[:i]
        assert acc.mean == pytest.approx(np.mean(prefix), rel=1e-9, abs=1e-9)
        if i >= 2:
            assert acc.population_variance == pytest.approx(np.var(prefix), rel=1e-9, abs=1e-8)


def test_empty_accumulator_defaults() -> None:
    acc = WelfordAccumulator()
    assert acc.count == 0
    assert acc.mean == 0.0
    assert acc.variance == 0.0
    assert acc.population_variance == 0.0
    assert acc.std == 0.0


def test_single_value_variance_is_zero_sample_but_defined_population() -> None:
    acc = WelfordAccumulator()
    acc.update(42.0)
    assert acc.mean == 42.0
    # Sample variance (ddof=1) is undefined with one point; we return 0.0.
    assert acc.variance == 0.0
    assert acc.population_variance == 0.0


def test_min_max_tracking() -> None:
    acc = WelfordAccumulator()
    for x in [5.0, 1.0, 9.0, -3.0, 4.0]:
        acc.update(x)
    stats = acc.stats()
    assert stats.min_value == -3.0
    assert stats.max_value == 9.0
    assert stats.count == 5


def test_reset() -> None:
    acc = WelfordAccumulator()
    acc.update(1.0)
    acc.update(2.0)
    acc.reset()
    assert acc.count == 0
    assert acc.mean == 0.0
