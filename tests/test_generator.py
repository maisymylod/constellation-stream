"""Unit tests for the deterministic event-time generator."""
from __future__ import annotations

from constellation_stream.config import GenConfig
from constellation_stream.generator import EPOCH_MS, stream
from constellation_stream.schema import ANOMALY_TYPES, CHANNEL_NAMES, NOMINAL


def test_stream_is_deterministic():
    cfg = GenConfig(n_satellites=10, samples_per_sat=30)
    a = list(stream(cfg))
    b = list(stream(cfg))
    assert len(a) == len(b)
    # Compare a stable projection (records carry the same ids and values).
    proj = lambda recs: [(r["record_id"], round(r["snr_db"], 6)) for r in recs]
    assert sorted(proj(a)) == sorted(proj(b))


def test_record_count_matches_config():
    cfg = GenConfig(n_satellites=10, samples_per_sat=30)
    recs = list(stream(cfg))
    assert len(recs) == cfg.total_records == 300


def test_record_ids_are_unique():
    cfg = GenConfig(n_satellites=15, samples_per_sat=40)
    ids = [r["record_id"] for r in stream(cfg)]
    assert len(ids) == len(set(ids))


def test_every_record_has_all_channels_and_keys():
    cfg = GenConfig(n_satellites=3, samples_per_sat=5)
    for r in stream(cfg):
        for ch in CHANNEL_NAMES:
            assert ch in r
        assert r["event_ts_ms"] >= EPOCH_MS
        assert "ingest_seq" in r
        assert r["anomaly_type"] in (NOMINAL, *ANOMALY_TYPES)


def test_late_records_arrive_out_of_event_time_order():
    cfg = GenConfig(n_satellites=40, samples_per_sat=120, late_fraction=0.05)
    recs = list(stream(cfg))
    # ingest_seq is monotonic (arrival order); event_ts is NOT, because some
    # records were deliberately delayed past later ones.
    seqs = [r["ingest_seq"] for r in recs]
    assert seqs == sorted(seqs)
    out_of_order = any(
        recs[i]["event_ts_ms"] > recs[i + 1]["event_ts_ms"] for i in range(len(recs) - 1)
    )
    assert out_of_order, "expected injected lateness to break event-time order"


def test_anomalies_are_injected_when_fraction_positive():
    cfg = GenConfig(n_satellites=50, samples_per_sat=200, anomaly_fraction=0.2)
    labels = {r["anomaly_type"] for r in stream(cfg)}
    assert labels & set(ANOMALY_TYPES), "expected at least one injected anomaly label"
