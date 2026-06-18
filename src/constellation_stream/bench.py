"""Benchmark harness: real ingest throughput + p99 query latency.

Measures two things and writes them to ``artifacts/bench.json`` and
``artifacts/BENCH.md``. Every number is produced by running the code. Nothing is
hand-written.

1. **Ingest / processing throughput** (records/sec): how fast the deterministic
   generator feeds the stateful :class:`StreamProcessor` (event-time windowing,
   watermarks, late-data handling) and the idempotent sink, end to end, in this
   process. This is the streaming hot path.

2. **Query p99 latency** (ms): an analytical query ("top anomalous windows per
   satellite") run repeatedly over the materialised window results, reporting
   p50 / p95 / p99. Run against the Iceberg/pyarrow table when available, else
   against the in-memory results, stated in the report either way.

Usage:

    python -m constellation_stream.bench --rows 5000000
    python -m constellation_stream.bench --sats 2400 --samples 3600 --query-iters 500

Scale is set by ``--rows`` (split across satellites) or explicit
``--sats``/``--samples``. The largest scale that completed in the build
environment is recorded in artifacts/BENCH.md and the README.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import time
from dataclasses import asdict

from .config import GenConfig
from .generator import stream
from .processor import StreamProcessor
from .sink import IdempotentSink


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def measure_throughput(cfg: GenConfig) -> tuple[dict, list[dict]]:
    """Push the whole stream through the processor + sink, timing the hot path.

    Returns (throughput summary, materialised window rows) so the query benchmark
    can reuse the same single run rather than processing the stream a second time.
    """
    proc = StreamProcessor()
    sink = IdempotentSink()
    t0 = time.perf_counter()
    for res in proc.process(stream(cfg)):
        sink.apply(res)
    dt = time.perf_counter() - t0
    rows = proc.stats.records_in
    summary = {
        "rows": rows,
        "seconds": dt,
        "throughput_rows_per_s": rows / dt if dt > 0 else 0.0,
        "windows": len(sink),
        "late_accepted": proc.stats.late_accepted,
        "dead_lettered": proc.stats.dead_lettered,
        "window_updates": proc.stats.window_updates,
    }
    return summary, sink.rows()


def _query_inmemory(rows: list[dict], iters: int) -> list[float]:
    """Top-anomaly-per-sat aggregation over in-memory window results."""
    lat = []
    for _ in range(iters):
        t0 = time.perf_counter()
        best: dict[str, dict] = {}
        for r in rows:
            if not r["is_anomaly"]:
                continue
            cur = best.get(r["sat_id"])
            if cur is None or r["temp_battery_c_max"] > cur["temp_battery_c_max"]:
                best[r["sat_id"]] = r
        _ = sorted(best.values(), key=lambda x: x["temp_battery_c_max"], reverse=True)[:20]
        lat.append((time.perf_counter() - t0) * 1000.0)
    return lat


def _query_iceberg(table, iters: int) -> list[float] | None:
    """Same query over the Iceberg table via pyarrow (scan + filter + aggregate)."""
    try:
        import pyarrow.compute as pc  # noqa: F401
    except Exception:
        return None
    lat = []
    for _ in range(iters):
        t0 = time.perf_counter()
        arrow = table.scan().to_arrow()
        mask = arrow.column("is_anomaly").to_pylist()
        rows = [
            {"sat_id": s, "m": m}
            for s, m, a in zip(
                arrow.column("sat_id").to_pylist(),
                arrow.column("temp_battery_c_max").to_pylist(),
                mask,
            )
            if a
        ]
        best: dict[str, float] = {}
        for r in rows:
            if r["sat_id"] not in best or (r["m"] or 0) > best[r["sat_id"]]:
                best[r["sat_id"]] = r["m"] or 0
        _ = sorted(best.values(), reverse=True)[:20]
        lat.append((time.perf_counter() - t0) * 1000.0)
    return lat


def measure_query_latency(rows: list[dict], iters: int, use_lake: bool) -> dict:
    """Time the analytical query over precomputed window ``rows``."""
    backend = "in-memory window results"
    lat: list[float] | None = None
    if use_lake:
        try:
            from .lake import load_or_create_table
            from .sink import commit_iceberg

            table = load_or_create_table()
            commit_iceberg(rows, table)
            lat = _query_iceberg(table, iters)
            if lat is not None:
                backend = "Apache Iceberg (pyarrow scan)"
        except Exception:
            lat = None
    if lat is None:
        lat = _query_inmemory(rows, iters)

    lat.sort()
    return {
        "backend": backend,
        "iters": iters,
        "result_rows": len(rows),
        "p50_ms": _percentile(lat, 0.50),
        "p95_ms": _percentile(lat, 0.95),
        "p99_ms": _percentile(lat, 0.99),
    }


def run(cfg: GenConfig, query_iters: int, use_lake: bool, outdir: str) -> dict:
    tp, rows = measure_throughput(cfg)
    ql = measure_query_latency(rows, query_iters, use_lake)
    report = {
        "config": asdict(cfg),
        "hardware": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "throughput": tp,
        "query_latency": ql,
    }
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "bench.json"), "w") as f:
        json.dump(report, f, indent=2)
    _write_md(report, os.path.join(outdir, "BENCH.md"))
    return report


def _write_md(report: dict, path: str) -> None:
    tp = report["throughput"]
    ql = report["query_latency"]
    hw = report["hardware"]
    lines = [
        "# Benchmark results",
        "",
        "Generated by `python -m constellation_stream.bench` (deterministic, seed 42).",
        "Every number here is produced by running the code; none is hand-written.",
        "",
        "## Scale actually run",
        "",
        f"- Records processed: **{tp['rows']:,}**",
        f"- Window results produced: **{tp['windows']:,}**",
        f"- Late records folded in (allowed-lateness): {tp['late_accepted']:,}",
        f"- Window updates re-emitted: {tp['window_updates']:,}",
        f"- Dead-lettered (beyond lateness horizon): {tp['dead_lettered']:,}",
        "",
        "## Ingest / processing throughput",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Records | {tp['rows']:,} |",
        f"| Wall time (s) | {tp['seconds']:.2f} |",
        f"| Throughput (records/s) | **{tp['throughput_rows_per_s']:,.0f}** |",
        "",
        "## Query latency (top anomalous window per satellite)",
        "",
        f"Backend: {ql['backend']} over {ql['result_rows']:,} window rows, "
        f"{ql['iters']} iterations.",
        "",
        "| Percentile | Latency (ms) |",
        "|---|---|",
        f"| p50 | {ql['p50_ms']:.3f} |",
        f"| p95 | {ql['p95_ms']:.3f} |",
        f"| p99 | **{ql['p99_ms']:.3f}** |",
        "",
        "## Hardware",
        "",
        f"- Platform: {hw['platform']}",
        f"- Machine: {hw['machine']}",
        f"- CPU count: {hw['cpu_count']}",
        f"- Python: {hw['python']}",
        "",
    ]
    with open(path, "w") as f:
        f.write("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=None, help="approx total rows (split across sats)")
    ap.add_argument("--sats", type=int, default=None)
    ap.add_argument("--samples", type=int, default=None)
    ap.add_argument("--query-iters", type=int, default=200)
    ap.add_argument("--lake", action="store_true", help="materialise to Iceberg and query it")
    ap.add_argument("--outdir", default="artifacts")
    args = ap.parse_args(argv)

    base = GenConfig()
    if args.rows is not None:
        sats = args.sats or base.n_satellites
        samples = max(1, args.rows // sats)
        cfg = GenConfig(n_satellites=sats, samples_per_sat=samples)
    else:
        cfg = GenConfig(
            n_satellites=args.sats or base.n_satellites,
            samples_per_sat=args.samples or base.samples_per_sat,
        )

    report = run(cfg, args.query_iters, args.lake, args.outdir)
    tp = report["throughput"]
    ql = report["query_latency"]
    print(f"rows={tp['rows']:,}  throughput={tp['throughput_rows_per_s']:,.0f} rec/s  "
          f"p99={ql['p99_ms']:.3f} ms  backend={ql['backend']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
