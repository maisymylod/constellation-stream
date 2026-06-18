"""Telemetry schema for the Heliosnet constellation (streaming edition).

A single source of truth for the telemetry channels, the subsystems they belong
to, and the anomaly taxonomy. The producer, the stream processor, the Iceberg
sink, the benchmark harness, and the Grafana queries all derive their column
lists from here so the data contract can never drift between components.

This is a vendored copy of the `constellation` sibling's channel schema and
anomaly taxonomy (nominal / drift / dropout / thermal_runaway). It is vendored
on purpose so this repo reproduces from a clean clone with no cross-repo import,
exactly as the rest of the Heliosnet suite does.
"""
from __future__ import annotations

from dataclasses import dataclass

# --- Anomaly taxonomy --------------------------------------------------------

NOMINAL = "nominal"
DRIFT = "drift"
DROPOUT = "dropout"
THERMAL_RUNAWAY = "thermal_runaway"

ANOMALY_TYPES = (DRIFT, DROPOUT, THERMAL_RUNAWAY)
ALL_LABELS = (NOMINAL, *ANOMALY_TYPES)


# --- Telemetry channels ------------------------------------------------------


@dataclass(frozen=True)
class Channel:
    name: str
    subsystem: str
    unit: str
    nominal: float  # baseline mean value
    noise: float  # 1-sigma gaussian noise on the baseline


# Ordered list of channels. The numeric channels are the anomaly feature space.
CHANNELS: tuple[Channel, ...] = (
    # Power
    Channel("bus_voltage_v", "power", "V", 28.0, 0.15),
    Channel("bus_current_a", "power", "A", 6.0, 0.30),
    Channel("battery_soc_pct", "power", "%", 82.0, 1.0),
    Channel("solar_array_w", "power", "W", 240.0, 8.0),
    # Thermal
    Channel("temp_battery_c", "thermal", "degC", 18.0, 0.8),
    Channel("temp_payload_c", "thermal", "degC", 24.0, 1.0),
    Channel("temp_radiator_c", "thermal", "degC", -12.0, 1.5),
    # Attitude / ADCS
    Channel("attitude_error_deg", "adcs", "deg", 0.05, 0.02),
    Channel("gyro_rate_dps", "adcs", "deg/s", 0.0, 0.05),
    # Link
    Channel("snr_db", "link", "dB", 14.0, 0.7),
    Channel("downlink_mbps", "link", "Mbps", 180.0, 12.0),
    Channel("packet_loss_pct", "link", "%", 0.4, 0.2),
    # Payload
    Channel("payload_power_w", "payload", "W", 95.0, 4.0),
    Channel("data_buffer_pct", "payload", "%", 35.0, 5.0),
)

CHANNEL_NAMES: tuple[str, ...] = tuple(c.name for c in CHANNELS)
SUBSYSTEMS: tuple[str, ...] = tuple(dict.fromkeys(c.subsystem for c in CHANNELS))

# Channels each anomaly type perturbs (used by the generator and documented in
# the README so the "real vs simulated" story is explicit).
ANOMALY_CHANNELS: dict[str, tuple[str, ...]] = {
    DRIFT: ("attitude_error_deg", "gyro_rate_dps"),
    DROPOUT: ("snr_db", "downlink_mbps", "packet_loss_pct"),
    THERMAL_RUNAWAY: ("temp_battery_c", "temp_payload_c", "bus_current_a"),
}

# --- Anomaly classification thresholds --------------------------------------
#
# The streaming job is stateful but rule-driven (not an ML model): it aggregates
# each channel over an event-time window and classifies the window against these
# thresholds. This keeps the windowed-aggregation semantics fully deterministic
# and unit-testable, which is the point of this repo. The `constellation` sibling
# is where the unsupervised ML detector lives.

THRESHOLDS = {
    DRIFT: {"channel": "attitude_error_deg", "stat": "max", "op": ">", "value": 0.5},
    DROPOUT: {"channel": "snr_db", "stat": "min", "op": "<", "value": 6.0},
    THERMAL_RUNAWAY: {"channel": "temp_battery_c", "stat": "max", "op": ">", "value": 30.0},
}


def classify_window(stats: dict[str, dict[str, float]]) -> str:
    """Classify a window of aggregated channel stats into the anomaly taxonomy.

    ``stats`` maps channel name -> {"min": .., "max": .., "avg": ..}. Returns one
    of NOMINAL / DRIFT / DROPOUT / THERMAL_RUNAWAY. Evaluated in a fixed priority
    order (thermal_runaway, dropout, drift) so the result is deterministic when
    more than one rule would fire.
    """
    order = (THERMAL_RUNAWAY, DROPOUT, DRIFT)
    for atype in order:
        rule = THRESHOLDS[atype]
        ch = rule["channel"]
        if ch not in stats:
            continue
        observed = stats[ch][rule["stat"]]
        if rule["op"] == ">" and observed > rule["value"]:
            return atype
        if rule["op"] == "<" and observed < rule["value"]:
            return atype
    return NOMINAL


__all__ = [
    "NOMINAL",
    "DRIFT",
    "DROPOUT",
    "THERMAL_RUNAWAY",
    "ANOMALY_TYPES",
    "ALL_LABELS",
    "Channel",
    "CHANNELS",
    "CHANNEL_NAMES",
    "SUBSYSTEMS",
    "ANOMALY_CHANNELS",
    "THRESHOLDS",
    "classify_window",
]
