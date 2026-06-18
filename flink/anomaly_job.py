"""PyFlink windowed-anomaly job (real Flink runtime).

This is the production stream-processing job. It implements the SAME semantics as
the cluster-free reference engine in
``src/constellation_stream/processor.py`` (event-time tumbling windows keyed by
satellite, a bounded-out-of-orderness watermark, allowed-lateness updates, and a
dead-letter side output for too-late records), but runs on a real Apache Flink
cluster reading from Kafka/Redpanda.

It is provided and runnable against the committed Docker stack
(``docker compose --profile flink``). It is NOT the source of the README headline
numbers: the build environment runs Python 3.14, for which no PyFlink wheel
exists, and a full Flink+Kafka cluster is heavier than the CI budget. The
honest, in-environment headline comes from the reference engine via
``make bench``; this job is the same logic at cluster fidelity. See the
"What is real vs simulated" section of the README.

Run (inside the Flink container, which ships a compatible Python):

    flink run -py /opt/flink/usrlib/anomaly_job.py
"""
from __future__ import annotations

import json
import os

# These imports resolve inside the Flink image (PyFlink installed there).
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# Window/watermark parameters mirror constellation_stream.config.WindowConfig.
WINDOW_MS = 30_000
WATERMARK_DELAY_MS = 2_000
ALLOWED_LATENESS_MS = 10_000

CHANNELS = [
    "bus_voltage_v", "bus_current_a", "battery_soc_pct", "solar_array_w",
    "temp_battery_c", "temp_payload_c", "temp_radiator_c",
    "attitude_error_deg", "gyro_rate_dps",
    "snr_db", "downlink_mbps", "packet_loss_pct",
    "payload_power_w", "data_buffer_pct",
]

# Classification thresholds mirror constellation_stream.schema.THRESHOLDS.
THRESHOLDS = [
    ("thermal_runaway", "temp_battery_c", "max", ">", 30.0),
    ("dropout", "snr_db", "min", "<", 6.0),
    ("drift", "attitude_error_deg", "max", ">", 0.5),
]


def classify(stats: dict) -> str:
    for atype, ch, stat, op, val in THRESHOLDS:
        observed = stats[ch][stat]
        if op == ">" and observed > val:
            return atype
        if op == "<" and observed < val:
            return atype
    return "nominal"


class _TsAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):  # noqa: ARG002
        return json.loads(value)["event_ts_ms"]


class WindowAggregate(ProcessWindowFunction):
    """Aggregate a window of telemetry records into one anomaly result row."""

    def process(self, key, context, elements):  # noqa: ARG002
        stats = {ch: {"min": None, "max": None, "sum": 0.0} for ch in CHANNELS}
        count = 0
        for raw in elements:
            rec = json.loads(raw)
            count += 1
            for ch in CHANNELS:
                v = rec[ch]
                s = stats[ch]
                s["sum"] += v
                s["min"] = v if s["min"] is None else min(s["min"], v)
                s["max"] = v if s["max"] is None else max(s["max"], v)
        agg = {ch: {"avg": s["sum"] / count, "min": s["min"], "max": s["max"]}
               for ch, s in stats.items()}
        w = context.window()
        out = {
            "sat_id": key,
            "window_start_ms": w.start,
            "window_end_ms": w.end,
            "sample_count": count,
            "anomaly_type": classify(agg),
            "result_key": f"{key}:{w.start}",
        }
        for ch in CHANNELS:
            for stat in ("avg", "min", "max"):
                out[f"{ch}_{stat}"] = agg[ch][stat]
        yield json.dumps(out)


def build_job(env: StreamExecutionEnvironment, bootstrap: str, topic: str):
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id("constellation-stream-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(
            __import__("pyflink.common.serialization", fromlist=["SimpleStringSchema"]).SimpleStringSchema()
        )
        .build()
    )
    wms = (
        WatermarkStrategy.for_bounded_out_of_orderness(Time.milliseconds(WATERMARK_DELAY_MS))
        .with_timestamp_assigner(_TsAssigner())
    )
    ds = env.from_source(source, wms, "telemetry")
    keyed = ds.key_by(lambda raw: json.loads(raw)["sat_id"], key_type=Types.STRING())
    windowed = (
        keyed.window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_MS)))
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .process(WindowAggregate(), Types.STRING())
    )
    return windowed


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    topic = os.environ.get("TELEMETRY_TOPIC", "heliosnet.telemetry")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "2")))
    results = build_job(env, bootstrap, topic)
    # Sink: print here for the demo. The Iceberg sink (exactly-once upsert) is
    # constellation_stream.sink.commit_iceberg, applied by the lake writer; see
    # docs/architecture.md for the FlinkSink->Iceberg wiring on a real cluster.
    results.print()
    env.execute("constellation-stream anomaly windows")


if __name__ == "__main__":
    main()
