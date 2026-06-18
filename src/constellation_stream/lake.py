"""Apache Iceberg lakehouse for window results.

Defines the Iceberg schema for the windowed anomaly results table and a helper to
load-or-create the table from a SQL catalog. Defaults to a local SQLite catalog
and a filesystem warehouse so it runs with zero external infrastructure; point
``WAREHOUSE`` / catalog env at MinIO (S3) for the distributed setup documented in
docs/architecture.md.

pyiceberg is imported lazily so unit tests that never touch the lake do not need
it installed.
"""
from __future__ import annotations

import os

from .config import warehouse_path
from .schema import CHANNEL_NAMES

NAMESPACE = "heliosnet"
TABLE = "telemetry_windows"
FULL_NAME = f"{NAMESPACE}.{TABLE}"


def iceberg_schema():
    """Build the pyiceberg Schema for the window-results table."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
    )

    fields = [
        NestedField(1, "sat_id", StringType(), required=True),
        NestedField(2, "window_start_ms", LongType(), required=True),
        NestedField(3, "window_end_ms", LongType(), required=True),
        NestedField(4, "sample_count", IntegerType(), required=True),
        NestedField(5, "late_count", IntegerType(), required=True),
        NestedField(6, "anomaly_type", StringType(), required=True),
        NestedField(7, "is_anomaly", IntegerType(), required=True),
        NestedField(8, "revision", IntegerType(), required=True),
        NestedField(9, "result_key", StringType(), required=True),
    ]
    fid = 10
    for ch in CHANNEL_NAMES:
        for stat in ("avg", "min", "max"):
            fields.append(NestedField(fid, f"{ch}_{stat}", DoubleType(), required=False))
            fid += 1
    # keep BooleanType imported for downstream parity; unused field id reserved
    _ = BooleanType
    return Schema(*fields)


def arrow_schema():
    """pyarrow schema matching :func:`iceberg_schema` exactly (types + nullability).

    pyiceberg refuses an append whose pyarrow schema does not match the table
    (required vs optional, int32 vs int64), so the sink builds its batches with
    this schema rather than letting pyarrow infer one.
    """
    import pyarrow as pa

    fields = [
        pa.field("sat_id", pa.string(), nullable=False),
        pa.field("window_start_ms", pa.int64(), nullable=False),
        pa.field("window_end_ms", pa.int64(), nullable=False),
        pa.field("sample_count", pa.int32(), nullable=False),
        pa.field("late_count", pa.int32(), nullable=False),
        pa.field("anomaly_type", pa.string(), nullable=False),
        pa.field("is_anomaly", pa.int32(), nullable=False),
        pa.field("revision", pa.int32(), nullable=False),
        pa.field("result_key", pa.string(), nullable=False),
    ]
    for ch in CHANNEL_NAMES:
        for stat in ("avg", "min", "max"):
            fields.append(pa.field(f"{ch}_{stat}", pa.float64(), nullable=True))
    return pa.schema(fields)


def load_catalog():
    """Load (or create) a pyiceberg SQL catalog backed by SQLite + filesystem."""
    from pyiceberg.catalog.sql import SqlCatalog

    wh = os.path.abspath(warehouse_path())
    os.makedirs(wh, exist_ok=True)
    catalog = SqlCatalog(
        "heliosnet",
        **{
            "uri": f"sqlite:///{os.path.join(wh, 'catalog.db')}",
            "warehouse": f"file://{wh}",
        },
    )
    return catalog


def load_or_create_table(catalog=None):
    """Return the Iceberg window-results table, creating namespace+table if needed.

    Partitioned by ``sat_id`` so per-satellite drilldown queries prune files.
    """
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    catalog = catalog or load_catalog()
    if (NAMESPACE,) not in [tuple(ns) for ns in catalog.list_namespaces()]:
        catalog.create_namespace(NAMESPACE)
    try:
        return catalog.load_table(FULL_NAME)
    except Exception:
        schema = iceberg_schema()
        spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="sat_id")
        )
        return catalog.create_table(FULL_NAME, schema=schema, partition_spec=spec)


__all__ = [
    "iceberg_schema",
    "arrow_schema",
    "load_catalog",
    "load_or_create_table",
    "FULL_NAME",
    "NAMESPACE",
    "TABLE",
]
