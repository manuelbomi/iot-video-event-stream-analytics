"""Unit tests for the synthetic event producer's simulation logic.

These test the pure simulation math (zone state stepping, spike
injection, event shape) without needing an MQTT broker.
"""

from __future__ import annotations

from src.producer.simulate_events import build_event, make_zone_states


def test_make_zone_states_creates_one_state_per_zone() -> None:
    zones = make_zone_states(["zone-1", "zone-2", "zone-3"], seed=1)
    assert [z.name for z in zones] == ["zone-1", "zone-2", "zone-3"]
    for z in zones:
        assert z.occupancy == z.baseline
        assert z.baseline > 0


def test_zone_step_returns_non_negative_entries_and_exits() -> None:
    zones = make_zone_states(["zone-1"], seed=2)
    zone = zones[0]
    for _ in range(50):
        entries, exits = zone.step()
        assert entries >= 0
        assert exits >= 0
        assert zone.occupancy >= 0.0


def test_zone_mean_reverts_toward_baseline() -> None:
    zones = make_zone_states(["zone-1"], seed=3)
    zone = zones[0]
    zone.occupancy = zone.baseline + 200  # push it far away

    for _ in range(200):
        zone.step()

    # After many steps, the mean-reverting walk should have pulled it back
    # much closer to baseline than the initial +200 displacement.
    assert abs(zone.occupancy - zone.baseline) < 50


def test_inject_spike_increases_occupancy() -> None:
    zones = make_zone_states(["zone-1"], seed=4)
    zone = zones[0]
    before = zone.occupancy
    zone.inject_spike(magnitude=6.0)
    assert zone.occupancy > before


def test_build_event_shape() -> None:
    zones = make_zone_states(["zone-1"], seed=5)
    zone = zones[0]
    event = build_event(zone, entries=2, exits=1)
    assert event["zone"] == "zone-1"
    assert event["entries"] == 2
    assert event["exits"] == 1
    assert event["event_type"] == "occupancy_update"
    assert "timestamp" in event
    assert isinstance(event["occupancy"], int)
