"""Unit tests for watermarks, allowed-lateness updates, and dead-lettering."""
from __future__ import annotations

from constellation_stream.config import WindowConfig
from constellation_stream.processor import StreamProcessor
from constellation_stream.schema import CHANNEL_NAMES


def _rec(sat: str, ts_ms: int, **over) -> dict:
    base = {ch: 0.0 for ch in CHANNEL_NAMES}
    base.update({"sat_id": sat, "event_ts_ms": ts_ms})
    base.update(over)
    return base


def test_late_record_within_allowed_lateness_updates_window():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=0, allowed_lateness_ms=30_000)
    recs = [
        _rec("A", 0, snr_db=10.0),
        _rec("A", 5_000, snr_db=10.0),
        _rec("A", 20_000),  # watermark -> 20000, fires window [0,10000)
        _rec("A", 3_000, snr_db=40.0),  # LATE: still within window [0,10000)
    ]
    proc = StreamProcessor(cfg)
    results = list(proc.process(recs))
    w0 = [r for r in results if r["window_start_ms"] == 0]
    # Two emissions for window 0: the original fire plus a late update.
    assert len(w0) == 2
    assert w0[0]["revision"] == 0
    assert w0[1]["revision"] == 1
    # The update folded the late record in: count went 2 -> 3, max snr rose.
    assert w0[0]["sample_count"] == 2
    assert w0[1]["sample_count"] == 3
    assert w0[1]["snr_db_max"] == 40.0
    assert w0[1]["late_count"] == 1
    assert proc.stats.late_accepted == 1
    assert proc.stats.window_updates == 1


def test_too_late_record_is_dead_lettered_not_dropped():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=0, allowed_lateness_ms=5_000)
    recs = [
        _rec("A", 0),
        _rec("A", 100_000),  # watermark -> 100000
        _rec("A", 1_000),  # window end 10000 < 100000-5000 horizon: too late
    ]
    proc = StreamProcessor(cfg)
    list(proc.process(recs))
    assert proc.stats.dead_lettered == 1
    assert len(proc.dead_letter) == 1
    assert proc.dead_letter[0]["event_ts_ms"] == 1_000


def test_watermark_trails_max_event_time_by_delay():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=3_000, allowed_lateness_ms=0)
    proc = StreamProcessor(cfg)
    # Step the generator one record at a time so we can observe the watermark
    # mid-stream, before the end-of-stream flush pushes it to +inf.
    gen = proc.process(iter([_rec("A", 5_000), _rec("A", 50_000), _rec("A", 50_001)]))
    next(gen)  # first window result fires once watermark passes a window end
    # By the time anything is yielded, the watermark trails the max event time
    # (50_000 or 50_001) by exactly the configured delay.
    assert proc.watermark in (50_000 - 3_000, 50_001 - 3_000)
    assert proc.watermark == proc.max_event_ts - cfg.watermark_delay_ms


def test_on_time_vs_late_accounting_sums_to_input():
    cfg = WindowConfig(size_ms=10_000, watermark_delay_ms=0, allowed_lateness_ms=60_000)
    recs = [
        _rec("A", 0),
        _rec("A", 20_000),  # fires [0,10000)
        _rec("A", 1_000),  # late, accepted into [0,10000)
        _rec("A", 2_000),  # late, accepted
    ]
    proc = StreamProcessor(cfg)
    list(proc.process(recs))
    total = proc.stats.on_time + proc.stats.late_accepted + proc.stats.dead_lettered
    assert total == proc.stats.records_in == 4
