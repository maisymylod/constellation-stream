"""End-to-end local pipeline: generate -> window -> idempotent sink -> Iceberg.

This is the cluster-free path that the demo, the small CI run, and the benchmark
share. It uses the in-process :class:`StreamProcessor` (same semantics as the
PyFlink job) and the idempotent sink, then commits window results into the
Apache Iceberg table.

    python -m constellation_stream.pipeline           # default profile -> Iceberg
    python -m constellation_stream.pipeline --no-lake # skip Iceberg write
"""
from __future__ import annotations

import argparse

from .config import GenConfig
from .generator import stream
from .processor import StreamProcessor
from .sink import IdempotentSink


def run_local(cfg: GenConfig | None = None, write_lake: bool = True) -> dict:
    """Run the full local pipeline; return a summary dict (no fabricated numbers)."""
    cfg = cfg or GenConfig()
    proc = StreamProcessor()
    sink = IdempotentSink()

    results = proc.process(stream(cfg))
    batch: list[dict] = []
    for res in results:
        sink.apply(res)
        batch.append(res)

    summary = {
        "records_in": proc.stats.records_in,
        "on_time": proc.stats.on_time,
        "late_accepted": proc.stats.late_accepted,
        "dead_lettered": proc.stats.dead_lettered,
        "windows_fired": proc.stats.windows_fired,
        "window_updates": proc.stats.window_updates,
        "sink_rows": len(sink),
        "anomalous_windows": sum(1 for r in sink.rows() if r["is_anomaly"]),
    }

    if write_lake:
        from .lake import load_or_create_table

        table = load_or_create_table()
        from .sink import commit_iceberg

        # Replace any prior contents for these windows: idempotent re-runs.
        commit_iceberg(sink.rows(), table)
        summary["lake_table"] = "heliosnet.telemetry_windows"

    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-lake", action="store_true", help="skip the Iceberg write")
    ap.add_argument("--sats", type=int, default=None, help="override satellite count")
    ap.add_argument("--samples", type=int, default=None, help="override samples per sat")
    args = ap.parse_args(argv)

    base = GenConfig()
    cfg = GenConfig(
        n_satellites=args.sats or base.n_satellites,
        samples_per_sat=args.samples or base.samples_per_sat,
    )
    summary = run_local(cfg, write_lake=not args.no_lake)
    for k, v in summary.items():
        print(f"{k:18s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
