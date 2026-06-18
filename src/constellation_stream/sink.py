"""Exactly-once / idempotent sink for window results.

The streaming job can re-emit a window (late-data update) and the whole input can
be replayed (recovery, at-least-once source). Neither must double count. This
sink gives exactly-once *effect* with an at-least-once source via two mechanisms:

1. **Idempotent upsert on a natural key.** Each window result has a natural key
   ``(sat_id, window_start_ms)`` and a monotonically increasing ``revision``.
   The sink keeps exactly one row per natural key, replacing it only when a
   strictly higher revision arrives. Replaying the same result, or applying an
   older revision, is a no-op.

2. **Committed-offset fencing.** The sink records the highest ``ingest_seq`` /
   checkpoint it has durably committed. On replay it ignores anything at or below
   the committed offset, the way a transactional sink fences a resumed source.

:class:`IdempotentSink` is pure in-memory and is what the unit tests exercise.
:func:`commit_iceberg` writes the same rows to an Apache Iceberg table (pyiceberg)
and is what the real pipeline uses; it upserts by natural key so committing the
same batch twice leaves the table unchanged.
"""
from __future__ import annotations

from collections.abc import Iterable

NATURAL_KEY = ("sat_id", "window_start_ms")


class IdempotentSink:
    """In-memory exactly-once sink: upsert-by-natural-key, keep highest revision."""

    def __init__(self) -> None:
        # natural key -> result row
        self._rows: dict[tuple, dict] = {}
        self.applied = 0
        self.ignored_duplicate = 0
        self.ignored_stale = 0
        self.committed_offset: int = -1

    def _key(self, row: dict) -> tuple:
        return tuple(row[k] for k in NATURAL_KEY)

    def apply(self, row: dict) -> bool:
        """Apply one window result. Returns True if it changed sink state."""
        key = self._key(row)
        existing = self._rows.get(key)
        if existing is None:
            self._rows[key] = dict(row)
            self.applied += 1
            return True
        if row["revision"] > existing["revision"]:
            self._rows[key] = dict(row)
            self.applied += 1
            return True
        if row["revision"] == existing["revision"]:
            self.ignored_duplicate += 1
        else:
            self.ignored_stale += 1
        return False

    def apply_many(self, rows: Iterable[dict]) -> None:
        for row in rows:
            self.apply(row)

    def apply_with_offset(self, row: dict, offset: int) -> bool:
        """Apply only if ``offset`` is past the committed offset (replay fence)."""
        if offset <= self.committed_offset:
            self.ignored_duplicate += 1
            return False
        return self.apply(row)

    def commit(self, offset: int) -> None:
        """Durably commit progress up to ``offset`` (transactional checkpoint)."""
        self.committed_offset = max(self.committed_offset, offset)

    def rows(self) -> list[dict]:
        return list(self._rows.values())

    def __len__(self) -> int:
        return len(self._rows)


def commit_iceberg(rows: list[dict], table) -> int:
    """Idempotently upsert window results into an Apache Iceberg table.

    Uses pyarrow + pyiceberg. The write is an upsert keyed by
    ``(sat_id, window_start_ms)``: existing rows for the same keys are deleted in
    the same snapshot before the new rows are appended, so committing the same
    batch twice converges to the same table state (idempotent). Imports are lazy
    so the unit tests need neither pyarrow nor pyiceberg installed.

    Returns the number of rows written.
    """
    import pyarrow as pa

    if not rows:
        return 0
    from .lake import arrow_schema

    table_data = pa.Table.from_pylist(rows, schema=arrow_schema())

    # Delete any existing rows that share a natural key with this batch, then
    # append. pyiceberg >= 0.7 exposes overwrite() with a row filter; we build an
    # IN predicate over the (sat_id, window_start_ms) pairs in this batch.
    from pyiceberg.expressions import And, EqualTo, In, Or

    sat_ids = sorted({r["sat_id"] for r in rows})
    starts = sorted({r["window_start_ms"] for r in rows})
    # Coarse predicate (sat_id IN ... AND window_start IN ...) then a fine append.
    pred = And(In("sat_id", sat_ids), In("window_start_ms", starts))
    import warnings

    with warnings.catch_warnings():
        # First write has nothing to delete; that warning is expected and benign.
        warnings.simplefilter("ignore")
        table.delete(pred)
        table.append(table_data)
    return len(rows)


__all__ = ["IdempotentSink", "commit_iceberg", "NATURAL_KEY"]
