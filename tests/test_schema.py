"""Unit tests for the anomaly taxonomy and window classification rules."""
from __future__ import annotations

from constellation_stream.schema import (
    CHANNEL_NAMES,
    DRIFT,
    DROPOUT,
    NOMINAL,
    THERMAL_RUNAWAY,
    classify_window,
)


def _stats(**over) -> dict:
    base = {ch: {"min": 0.0, "max": 0.0, "avg": 0.0} for ch in CHANNEL_NAMES}
    # Set nominal-ish defaults that trip no rule.
    base["attitude_error_deg"] = {"min": 0.0, "max": 0.05, "avg": 0.02}
    base["snr_db"] = {"min": 14.0, "max": 15.0, "avg": 14.5}
    base["temp_battery_c"] = {"min": 17.0, "max": 19.0, "avg": 18.0}
    base.update(over)
    return base


def test_nominal_when_no_rule_trips():
    assert classify_window(_stats()) == NOMINAL


def test_drift_detected_on_high_attitude_error():
    s = _stats(attitude_error_deg={"min": 0.0, "max": 0.9, "avg": 0.5})
    assert classify_window(s) == DRIFT


def test_dropout_detected_on_low_snr():
    s = _stats(snr_db={"min": 2.0, "max": 14.0, "avg": 8.0})
    assert classify_window(s) == DROPOUT


def test_thermal_runaway_detected_on_high_battery_temp():
    s = _stats(temp_battery_c={"min": 18.0, "max": 45.0, "avg": 30.0})
    assert classify_window(s) == THERMAL_RUNAWAY


def test_priority_thermal_beats_others_when_multiple_trip():
    s = _stats(
        attitude_error_deg={"min": 0.0, "max": 0.9, "avg": 0.5},
        snr_db={"min": 2.0, "max": 14.0, "avg": 8.0},
        temp_battery_c={"min": 18.0, "max": 45.0, "avg": 30.0},
    )
    assert classify_window(s) == THERMAL_RUNAWAY
