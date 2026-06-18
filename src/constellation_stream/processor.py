"""Stateful event-time stream processor (cluster-free reference engine).

This is a small, dependency-free streaming engine that implements the same
semantics a PyFlink job would, so the windowing / watermark / late-data /
exactly-once logic is deterministic and unit-testable in CI without a cluster.
The equivalent PyFlink job (identical semantics, real Flink runtime) lives in
``flink/anomaly_job.py`` and runs against the committed Docker stack.

Semantics implemented:

- **Event-time tumbling windows** keyed by ``sat_id`` over ``event_ts_ms``.
- **Watermarks** with bounded out-of-orderness: the watermark trails the max
  observed event time by ``watermark_delay_ms``. A window fires when the
  watermark passes ``window_end``.
- **Allowed lateness**: after a window fires, a late record whose event time
  still falls inside that window updates the window and re-emits an *updated*
  result, for up to ``allowed_lateness_ms`` past the window end.
- **Dead-letter for too-late data**: records later than the allowed-lateness
  horizon are routed to a dead-letter list, never silently dropped.
- **Exactly-once / idempotent sink**: every window result carries a stable
  ``result_key`` (sat_id + window_start + revision). The sink dedupes on the
  natural key (sat_id, window_start) and keeps the highest revision, so replaying
  the input or re-firing a window can never double count. See :mod:`.sink`.

Each emitted window result has the shape consumed by the Iceberg sink and the
Grafana queries.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from .config import WINDOW, WindowConfig
from .schema import CHANNEL_NAMES, classify_window


@dataclass
class _WindowState:
    """Running aggregation for one (sat_id, window_start) window."""

    sat_id: str
    window_start: int
    window_end: int
    count: int = 0
    late_count: int = 0
    sums: dict[str, float] = field(default_factory=dict)
    mins: dict[str, float] = field(default_factory=dict)
    maxs: dict[str, float] = field(default_factory=dict)
    fired: bool = False
    revision: int = 0

    def add(self, rec: dict, *, late: bool) -> None:
        self.count += 1
        if late:
            self.late_count += 1
        for ch in CHANNEL_NAMES:
            v = rec[ch]
            self.sums[ch] = self.sums.get(ch, 0.0) + v
            self.mins[ch] = v if ch not in self.mins else min(self.mins[ch], v)
            self.maxs[ch] = v if ch not in self.maxs else max(self.maxs[ch], v)

    def result(self) -> dict:
        stats = {
            ch: {
                "avg": self.sums[ch] / self.count,
                "min": self.mins[ch],
                "max": self.maxs[ch],
            }
            for ch in CHANNEL_NAMES
        }
        anomaly_type = classify_window(stats)
        out = {
            "sat_id": self.sat_id,
            "window_start_ms": self.window_start,
            "window_end_ms": self.window_end,
            "sample_count": self.count,
            "late_count": self.late_count,
            "anomaly_type": anomaly_type,
            "is_anomaly": int(anomaly_type != "nominal"),
            "revision": self.revision,
            # Natural key + revision => stable identity for the idempotent sink.
            "result_key": f"{self.sat_id}:{self.window_start}",
        }
        for ch in CHANNEL_NAMES:
            out[f"{ch}_avg"] = stats[ch]["avg"]
            out[f"{ch}_min"] = stats[ch]["min"]
            out[f"{ch}_max"] = stats[ch]["max"]
        return out


def _window_bounds(event_ts_ms: int, size_ms: int) -> tuple[int, int]:
    start = (event_ts_ms // size_ms) * size_ms
    return start, start + size_ms


@dataclass
class ProcessorStats:
    records_in: int = 0
    on_time: int = 0
    late_accepted: int = 0
    dead_lettered: int = 0
    windows_fired: int = 0
    window_updates: int = 0


class StreamProcessor:
    """Event-time windowed anomaly aggregator with watermark + late-data handling.

    Drive it with :meth:`process` (an iterable of records). It yields window
    *results* as they fire and as late data updates them. Too-late records are
    collected in :attr:`dead_letter`. The processor is single-pass and keeps only
    open windows in memory, so it scales to large streams.
    """

    def __init__(self, cfg: WindowConfig | None = None):
        self.cfg = cfg or WINDOW
        self.windows: dict[tuple[str, int], _WindowState] = {}
        self.fired_results: dict[tuple[str, int], dict] = {}
        self.dead_letter: list[dict] = []
        self.watermark: int = -(1 << 62)
        self.max_event_ts: int = -(1 << 62)
        self.stats = ProcessorStats()

    # -- watermark --------------------------------------------------------
    def _advance_watermark(self, event_ts_ms: int) -> None:
        self.max_event_ts = max(self.max_event_ts, event_ts_ms)
        self.watermark = self.max_event_ts - self.cfg.watermark_delay_ms

    def _fire_ready(self) -> Iterator[dict]:
        """Fire every open window whose end is at or below the watermark."""
        ready = [
            key
            for key, w in self.windows.items()
            if not w.fired and w.window_end <= self.watermark
        ]
        for key in sorted(ready, key=lambda k: (k[1], k[0])):
            w = self.windows[key]
            w.fired = True
            self.stats.windows_fired += 1
            res = w.result()
            self.fired_results[key] = res
            yield res

    def _evict_expired(self) -> None:
        """Drop windows past the allowed-lateness horizon to bound memory."""
        horizon = self.watermark - self.cfg.allowed_lateness_ms
        dead = [
            key
            for key, w in self.windows.items()
            if w.fired and w.window_end < horizon
        ]
        for key in dead:
            del self.windows[key]
            self.fired_results.pop(key, None)

    # -- main loop --------------------------------------------------------
    def process(self, records: Iterable[dict]) -> Iterator[dict]:
        size = self.cfg.size_ms
        for rec in records:
            self.stats.records_in += 1
            ets = rec["event_ts_ms"]
            ws, we = _window_bounds(ets, size)
            key = (rec["sat_id"], ws)

            too_late = we <= (self.watermark - self.cfg.allowed_lateness_ms)
            if too_late:
                # Beyond the allowed-lateness horizon: dead-letter, never drop.
                self.stats.dead_lettered += 1
                self.dead_letter.append(rec)
                self._advance_watermark(ets)
                continue

            is_late = we <= self.watermark
            w = self.windows.get(key)
            if w is None:
                w = _WindowState(rec["sat_id"], ws, we)
                self.windows[key] = w

            if is_late:
                self.stats.late_accepted += 1
            else:
                self.stats.on_time += 1
            w.add(rec, late=is_late)

            if is_late and w.fired:
                # Re-emit an updated result for an already-fired window.
                w.revision += 1
                self.stats.window_updates += 1
                res = w.result()
                self.fired_results[key] = res
                self._advance_watermark(ets)
                yield res
                continue

            self._advance_watermark(ets)
            yield from self._fire_ready()
            self._evict_expired()

        # End of stream: advance watermark to +inf and flush every open window.
        self.watermark = 1 << 62
        yield from self._fire_ready()


def run(records: Iterable[dict], cfg: WindowConfig | None = None) -> tuple[list[dict], StreamProcessor]:
    """Convenience: run the processor to completion, return (results, processor)."""
    proc = StreamProcessor(cfg)
    results = list(proc.process(records))
    return results, proc


__all__ = ["StreamProcessor", "ProcessorStats", "run"]
