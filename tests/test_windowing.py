"""Unit tests for event-time tumbling windows and watermark firing."""
from __future__ import annotations

from constellation_stream.config import WindowConfig
from constellation_stream.processor import StreamProcessor, _window_bounds
from constellation_stream.schema import CHANNEL_NAMES


def _rec(sat: str, ts_ms: int, **over) -> dict:
    base = {ch: 0.0 for ch in CHANNEL_NAMES}
    base.update({"sat_id": sat, "event_ts_ms": ts_ms})
    base.update(over)
    return base


def test_window_bounds_align_to_size():
    assert _window_bounds(0, 30_000) == (0, 30_000)
    assert _window_bounds(29_999, 30_000) == (0, 30_000)
    assert _window_bounds(30_000, 30_000) == (30_000, 60_000)
    assert _window_bounds(75_000, 30_000) == (60_000, 90_000)


def test_records_group_into_correct_windows():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=0, allowed_lateness_ms=0)
    recs = [
        _rec("A", 0),
        _rec("A", 5_000),
        _rec("A", 9_999),
        _rec("A", 10_000),  # next window; its arrival fires window [0,10000)
        _rec("A", 50_000),  # advances watermark, fires [10000,20000)
    ]
    proc = StreamProcessor(cfg)
    results = list(proc.process(recs))
    first = [r for r in results if r["window_start_ms"] == 0]
    assert len(first) == 1
    assert first[0]["sample_count"] == 3  # 0, 5000, 9999


def test_aggregates_min_max_avg():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=0, allowed_lateness_ms=0)
    recs = [
        _rec("A", 0, snr_db=10.0),
        _rec("A", 1_000, snr_db=20.0),
        _rec("A", 2_000, snr_db=30.0),
        _rec("A", 100_000, snr_db=0.0),  # flush the first window
    ]
    proc = StreamProcessor(cfg)
    results = list(proc.process(recs))
    w = next(r for r in results if r["window_start_ms"] == 0)
    assert w["snr_db_min"] == 10.0
    assert w["snr_db_max"] == 30.0
    assert abs(w["snr_db_avg"] - 20.0) < 1e-9


def test_per_satellite_keying():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=0, allowed_lateness_ms=0)
    recs = [_rec("A", 0), _rec("B", 0), _rec("A", 50_000), _rec("B", 50_000)]
    proc = StreamProcessor(cfg)
    results = list(proc.process(recs))
    sats = {(r["sat_id"], r["window_start_ms"]) for r in results}
    assert ("A", 0) in sats
    assert ("B", 0) in sats


def test_end_of_stream_flushes_open_windows():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=5_000, allowed_lateness_ms=0)
    recs = [_rec("A", 0), _rec("A", 1_000)]  # never advances watermark past end
    proc = StreamProcessor(cfg)
    results = list(proc.process(recs))
    # The window only fires on the final flush.
    assert len(results) == 1
    assert results[0]["sample_count"] == 2
