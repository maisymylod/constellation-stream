"""Tests for the end-to-end local pipeline, the benchmark harness, and export.

These exercise the wiring (generate -> window -> sink -> export/benchmark) without
a cluster. The Iceberg lake path is covered separately and only when pyiceberg is
installed (CI's pipeline job), so the core test job needs no native wheels.
"""
from __future__ import annotations

import json

import pytest

from constellation_stream.bench import _percentile, measure_query_latency, measure_throughput, run
from constellation_stream.config import GenConfig
from constellation_stream.export import export
from constellation_stream.pipeline import run_local


SMALL = GenConfig(n_satellites=20, samples_per_sat=120)


def test_percentile_interpolates():
    vals = [0.0, 1.0, 2.0, 3.0, 4.0]
    assert _percentile(vals, 0.0) == 0.0
    assert _percentile(vals, 1.0) == 4.0
    assert _percentile(vals, 0.5) == 2.0


def test_pipeline_no_lake_summary_is_consistent():
    s = run_local(SMALL, write_lake=False)
    assert s["records_in"] == SMALL.total_records
    assert s["sink_rows"] == s["windows_fired"]
    assert s["on_time"] + s["late_accepted"] + s["dead_lettered"] == s["records_in"]
    assert "lake_table" not in s


def test_measure_throughput_reports_real_numbers():
    tp, rows = measure_throughput(SMALL)
    assert tp["rows"] == SMALL.total_records
    assert tp["seconds"] > 0
    assert tp["throughput_rows_per_s"] > 0
    assert tp["windows"] > 0
    assert len(rows) == tp["windows"]


def test_measure_query_latency_inmemory():
    _, rows = measure_throughput(SMALL)
    ql = measure_query_latency(rows, iters=5, use_lake=False)
    assert ql["backend"] == "in-memory window results"
    assert ql["p50_ms"] >= 0
    assert ql["p99_ms"] >= ql["p50_ms"]
    assert ql["result_rows"] > 0


def test_run_writes_artifacts(tmp_path):
    report = run(SMALL, query_iters=5, use_lake=False, outdir=str(tmp_path))
    assert (tmp_path / "bench.json").exists()
    assert (tmp_path / "BENCH.md").exists()
    saved = json.loads((tmp_path / "bench.json").read_text())
    assert saved["throughput"]["rows"] == SMALL.total_records
    assert "p99_ms" in saved["query_latency"]


def test_export_writes_window_json(tmp_path):
    out = tmp_path / "windows.json"
    n = export(SMALL, out=str(out))
    rows = json.loads(out.read_text())
    assert len(rows) == n > 0
    assert {"sat_id", "window_start_ms", "anomaly_type", "is_anomaly"} <= set(rows[0])


def test_pipeline_into_iceberg_is_idempotent(tmp_path, monkeypatch):
    """Lake path: write to Iceberg twice; row count must not change. Needs pyiceberg."""
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.setenv("WAREHOUSE", str(tmp_path / "wh"))
    from constellation_stream.lake import load_or_create_table

    run_local(SMALL, write_lake=True)
    n1 = load_or_create_table().scan().to_arrow().num_rows
    run_local(SMALL, write_lake=True)
    n2 = load_or_create_table().scan().to_arrow().num_rows
    assert n1 == n2 > 0
