"""Export window results to a static JSON the Grafana dashboards read.

Runs the local pipeline (or reads the Iceberg table when available) and writes
``grafana/data/windows.json``: a flat array of window-result rows the committed
Grafana dashboards render through the Infinity datasource. No fabricated rows.

    python -m constellation_stream.export
"""
from __future__ import annotations

import argparse
import json
import os

from .config import GenConfig
from .generator import stream
from .processor import StreamProcessor
from .sink import IdempotentSink

DEFAULT_OUT = "grafana/data/windows.json"


def export(cfg: GenConfig | None = None, out: str = DEFAULT_OUT) -> int:
    cfg = cfg or GenConfig()
    proc = StreamProcessor()
    sink = IdempotentSink()
    for res in proc.process(stream(cfg)):
        sink.apply(res)
    rows = sorted(sink.rows(), key=lambda r: (r["sat_id"], r["window_start_ms"]))
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as f:
        json.dump(rows, f)
    print(f"wrote {len(rows)} window rows to {out}")
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--sats", type=int, default=None)
    ap.add_argument("--samples", type=int, default=None)
    args = ap.parse_args(argv)
    base = GenConfig()
    cfg = GenConfig(
        n_satellites=args.sats or base.n_satellites,
        samples_per_sat=args.samples or base.samples_per_sat,
    )
    export(cfg, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
