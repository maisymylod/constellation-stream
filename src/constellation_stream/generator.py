"""Deterministic, event-time telemetry generator for the streaming pipeline.

Each record is a dict carrying:

- ``sat_id``      partition / grouping key
- ``event_ts_ms`` event time in epoch milliseconds (NOT wall clock)
- ``ingest_seq``  monotonic emission order (the order records hit the processor)
- one field per channel in :mod:`constellation_stream.schema`
- ``anomaly_type`` ground-truth label (the simulated truth)
- ``record_id``   deterministic unique id (used by the exactly-once sink)

The *inputs* (telemetry) are simulated. The *outputs* (window counts, throughput,
p99) are computed by running real code over this stream, never hand-written.

Each satellite's full timeline is computed in one vectorised numpy pass (so the
generator is fast enough to feed millions of records through the processor), then
the records are interleaved across satellites in event-time order, with a
fraction held back and re-released out of order to exercise late-data handling.
Per-satellite RNG is derived from a :class:`numpy.random.SeedSequence` so the
output is deterministic and independent of iteration order.
"""
from __future__ import annotations

from collections.abc import Iterator

import numpy as np

from .config import GenConfig
from .schema import (
    ANOMALY_TYPES,
    CHANNELS,
    CHANNEL_NAMES,
    DRIFT,
    DROPOUT,
    NOMINAL,
    THERMAL_RUNAWAY,
)

# Fixed event-time origin => deterministic, no wall-clock dependence.
EPOCH_MS = int(np.datetime64("2026-01-01T00:00:00").astype("datetime64[ms]").astype("int64"))
_ORBIT_PERIOD_MS = 5_400_000.0  # ~90 min LEO orbit


def _anomaly_plan(cfg: GenConfig) -> tuple[dict[int, str], dict[int, int]]:
    """Deterministic anomaly assignment: sat -> type and sat -> start sample."""
    rng = np.random.default_rng(cfg.seed)
    n, m = cfg.n_satellites, cfg.samples_per_sat
    n_anom = int(round(n * cfg.anomaly_fraction))
    anom_sats = rng.choice(n, size=n_anom, replace=False)
    types = rng.choice(ANOMALY_TYPES, size=n_anom)
    anom_type: dict[int, str] = {}
    anom_start: dict[int, int] = {}
    for sat, atype in zip(sorted(anom_sats.tolist()), types):
        anom_type[sat] = str(atype)
        anom_start[sat] = int(rng.integers(low=m // 4, high=max(m // 4 + 1, m - m // 5)))
    return anom_type, anom_start


def _sat_timeline(cfg: GenConfig, sat: int, anom_type: str, anom_start: int) -> dict[str, np.ndarray]:
    """Vectorised telemetry for one satellite across all samples (channel -> array)."""
    m = cfg.samples_per_sat
    # Per-satellite RNG, reproducible and order-independent.
    rng = np.random.default_rng(np.random.SeedSequence([cfg.seed, sat]))
    t_ms = np.arange(m) * cfg.sample_period_ms
    phase = rng.uniform(0, 2 * np.pi)
    orbit = np.sin(2 * np.pi * t_ms / _ORBIT_PERIOD_MS + phase)

    out: dict[str, np.ndarray] = {}
    for ch in CHANNELS:
        offset = rng.normal(0.0, ch.noise)
        signal = np.full(m, ch.nominal + offset) + rng.normal(0.0, ch.noise, size=m)
        if ch.name == "solar_array_w":
            signal += 60.0 * np.clip(orbit, 0, None)
        elif ch.name in ("temp_battery_c", "temp_radiator_c", "temp_payload_c"):
            signal += 3.0 * orbit
        elif ch.name == "battery_soc_pct":
            signal += 4.0 * orbit
        out[ch.name] = signal

    if anom_type != NOMINAL:
        dur = m - anom_start
        ramp = np.zeros(m)
        ramp[anom_start:] = np.linspace(0.0, 1.0, dur)
        if anom_type == DRIFT:
            out["attitude_error_deg"] += ramp * 1.2
            out["gyro_rate_dps"] += ramp * 0.4
        elif anom_type == DROPOUT:
            out["snr_db"] -= ramp * 11.0
            out["downlink_mbps"] *= 1.0 - ramp * 0.92
            out["packet_loss_pct"] += ramp * 55.0
        elif anom_type == THERMAL_RUNAWAY:
            out["temp_battery_c"] += np.expm1(ramp * 2.2) * 6.0
            out["temp_payload_c"] += np.expm1(ramp * 1.6) * 4.0
            out["bus_current_a"] += ramp * 3.0

    out["battery_soc_pct"] = np.clip(out["battery_soc_pct"], 0, 100)
    out["packet_loss_pct"] = np.clip(out["packet_loss_pct"], 0, 100)
    out["downlink_mbps"] = np.clip(out["downlink_mbps"], 0, None)
    out["data_buffer_pct"] = np.clip(out["data_buffer_pct"], 0, 100)
    return out


def stream(cfg: GenConfig | None = None) -> Iterator[dict]:
    """Yield telemetry records in (mostly) event-time order, with injected lateness.

    Records are emitted sample-by-sample across all satellites (so within a sample
    index the stream is event-time ordered). A fraction ``cfg.late_fraction`` of
    records are held back and re-released a few samples later, simulating
    out-of-order arrival. ``ingest_seq`` reflects true (arrival) order.
    """
    cfg = cfg or GenConfig()
    anom_type_map, anom_start_map = _anomaly_plan(cfg)
    late_rng = np.random.default_rng(cfg.seed + 1)
    n, m = cfg.n_satellites, cfg.samples_per_sat

    # Precompute each satellite's vectorised timeline once.
    timelines = [
        _sat_timeline(cfg, sat, anom_type_map.get(sat, NOMINAL), anom_start_map.get(sat, 1 << 30))
        for sat in range(n)
    ]
    sat_ids = [f"HEL-{sat:04d}" for sat in range(n)]
    skew_samples = max(2, cfg.late_max_skew_ms // cfg.sample_period_ms)

    def build(sat: int, k: int) -> dict:
        tl = timelines[sat]
        rec = {ch: float(tl[ch][k]) for ch in CHANNEL_NAMES}
        atype = anom_type_map.get(sat, NOMINAL)
        in_anom = atype != NOMINAL and k >= anom_start_map.get(sat, 1 << 30)
        rec["sat_id"] = sat_ids[sat]
        rec["event_ts_ms"] = EPOCH_MS + k * cfg.sample_period_ms
        rec["anomaly_type"] = atype if in_anom else NOMINAL
        rec["record_id"] = f"{sat_ids[sat]}:{k:08d}"
        return rec

    seq = 0
    pending_late: list[dict] = []
    for k in range(m):
        for sat in range(n):
            if k > 0 and late_rng.random() < cfg.late_fraction:
                rec = build(sat, k)
                rec["_release_at"] = k + int(late_rng.integers(1, skew_samples))
                pending_late.append(rec)
                continue
            rec = build(sat, k)
            rec["ingest_seq"] = seq
            seq += 1
            yield rec
        still: list[dict] = []
        for r in pending_late:
            if r["_release_at"] <= k:
                del r["_release_at"]
                r["ingest_seq"] = seq
                seq += 1
                yield r
            else:
                still.append(r)
        pending_late = still

    for r in pending_late:
        r.pop("_release_at", None)
        r["ingest_seq"] = seq
        seq += 1
        yield r


__all__ = ["stream", "EPOCH_MS"]
