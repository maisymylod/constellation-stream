"""Configuration for the streaming telemetry pipeline.

Everything that affects generated data or windowing is captured here and seeded,
so the benchmark and the unit tests produce identical results on any machine.
The README's headline numbers are reproducible because of this.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GenConfig:
    """Parameters for a deterministic telemetry stream.

    The generator is event-time aware: each record carries an event timestamp
    derived from a fixed epoch plus its sample index, and a fraction of records
    are deliberately delayed past the watermark to exercise late-data handling.
    """

    n_satellites: int = 200
    samples_per_sat: int = 600  # event-time samples per satellite
    sample_period_ms: int = 1000  # event-time spacing between a sat's samples
    seed: int = 42
    anomaly_fraction: float = 0.08  # fraction of satellites carrying an anomaly
    late_fraction: float = 0.02  # fraction of records emitted out of order / late
    late_max_skew_ms: int = 12_000  # how far back a late record's event time sits

    @property
    def total_records(self) -> int:
        return self.n_satellites * self.samples_per_sat


@dataclass(frozen=True)
class WindowConfig:
    """Event-time tumbling-window + watermark configuration.

    ``size_ms`` is the tumbling window width. ``allowed_lateness_ms`` is how long
    after a window closes a late record is still folded into that window's result
    (an update is emitted); records later than that are routed to the dead-letter
    stream rather than silently dropped.
    """

    size_ms: int = 30_000  # 30s tumbling windows
    watermark_delay_ms: int = 2_000  # bounded-out-of-orderness watermark slack
    allowed_lateness_ms: int = 10_000  # update windows for this long after close


# Default windowing used by the job, the benchmark, and the tests.
WINDOW = WindowConfig()

# Larger profile referenced in docs. Kept off the default path so the benchmark
# and CI stay fast; the README documents how to scale it toward billions.
FLEET_CONFIG = GenConfig(n_satellites=2400, samples_per_sat=3600)


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


def telemetry_topic() -> str:
    return os.environ.get("TELEMETRY_TOPIC", "heliosnet.telemetry")


def warehouse_path() -> str:
    """Filesystem warehouse root for the Iceberg lake (local default)."""
    return os.environ.get("WAREHOUSE", "./warehouse")
