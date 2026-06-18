"""Unit tests for the exactly-once / idempotent sink."""
from __future__ import annotations

from constellation_stream.sink import IdempotentSink


def _row(sat: str, ws: int, revision: int = 0, count: int = 1) -> dict:
    return {
        "sat_id": sat,
        "window_start_ms": ws,
        "window_end_ms": ws + 30_000,
        "sample_count": count,
        "revision": revision,
        "is_anomaly": 0,
        "result_key": f"{sat}:{ws}",
    }


def test_first_apply_inserts():
    sink = IdempotentSink()
    assert sink.apply(_row("A", 0)) is True
    assert len(sink) == 1
    assert sink.applied == 1


def test_duplicate_same_revision_is_noop():
    sink = IdempotentSink()
    sink.apply(_row("A", 0, revision=0, count=2))
    changed = sink.apply(_row("A", 0, revision=0, count=2))
    assert changed is False
    assert len(sink) == 1
    assert sink.ignored_duplicate == 1
    # The single stored row is unchanged: no double counting.
    assert sink.rows()[0]["sample_count"] == 2


def test_higher_revision_replaces():
    sink = IdempotentSink()
    sink.apply(_row("A", 0, revision=0, count=2))
    assert sink.apply(_row("A", 0, revision=1, count=3)) is True
    assert len(sink) == 1
    assert sink.rows()[0]["sample_count"] == 3
    assert sink.rows()[0]["revision"] == 1


def test_stale_revision_is_ignored():
    sink = IdempotentSink()
    sink.apply(_row("A", 0, revision=2, count=5))
    assert sink.apply(_row("A", 0, revision=1, count=99)) is False
    assert sink.ignored_stale == 1
    assert sink.rows()[0]["sample_count"] == 5  # stale update did not land


def test_full_replay_is_idempotent():
    """Replaying the entire output stream twice yields identical sink state."""
    stream = [_row("A", 0), _row("B", 0), _row("A", 30_000), _row("A", 0, revision=1, count=4)]

    sink1 = IdempotentSink()
    sink1.apply_many(stream)
    state1 = sorted((r["sat_id"], r["window_start_ms"], r["revision"], r["sample_count"]) for r in sink1.rows())

    sink2 = IdempotentSink()
    sink2.apply_many(stream)
    sink2.apply_many(stream)  # replay
    state2 = sorted((r["sat_id"], r["window_start_ms"], r["revision"], r["sample_count"]) for r in sink2.rows())

    assert state1 == state2
    assert len(sink2) == 3  # (A,0), (B,0), (A,30000)


def test_offset_fence_blocks_replayed_records():
    sink = IdempotentSink()
    assert sink.apply_with_offset(_row("A", 0), offset=10) is True
    sink.commit(10)
    # A resumed source replays offset 10 again: must be fenced out.
    assert sink.apply_with_offset(_row("A", 30_000), offset=10) is False
    assert sink.apply_with_offset(_row("A", 60_000), offset=11) is True


def test_pipeline_late_update_does_not_double_count(tmp_path, monkeypatch):
    """End-to-end: the in-process pipeline's late updates must not inflate counts."""
    from constellation_stream.config import GenConfig
    from constellation_stream.pipeline import run_local

    summary = run_local(GenConfig(n_satellites=20, samples_per_sat=120), write_lake=False)
    # Exactly one sink row per fired window, even though some windows were updated.
    assert summary["sink_rows"] == summary["windows_fired"]
    assert summary["window_updates"] >= 0
