# Architecture

`constellation-stream` is the production-scale streaming evolution of the
[`constellation`](https://github.com/maisymylod/constellation) data plane. Where
`constellation` batches a Kafka topic into TimescaleDB and runs an unsupervised
ML detector, this repo turns the same Heliosnet telemetry into a stateful,
event-time stream-processing pipeline landing in an Apache Iceberg lakehouse.

## Data flow

```
                        Heliosnet telemetry (simulated, labelled)
                                       |
                            generator.py  (deterministic, event-time,
                                       |    injects out-of-order / late records)
                                       v
          +--------------------------------------------------------------+
          | Kafka / Redpanda  topic: heliosnet.telemetry                 |
          | keyed by sat_id (a partition holds a satellite's history)    |
          +--------------------------------------------------------------+
                                       |
                                       v
          +--------------------------------------------------------------+
          | Stream processor  (stateful, event-time)                     |
          |                                                              |
          |  - tumbling 30s windows keyed by sat_id                      |
          |  - bounded-out-of-orderness watermark (2s)                   |
          |  - allowed lateness (10s): late records UPDATE a fired       |
          |    window and re-emit a higher-revision result               |
          |  - too-late records -> dead-letter (never dropped)           |
          |  - windowed anomaly classification over the taxonomy         |
          |    (nominal / drift / dropout / thermal_runaway)             |
          |                                                              |
          |  Two interchangeable runtimes, identical semantics:          |
          |   (a) flink/anomaly_job.py  -> real Apache Flink cluster     |
          |   (b) src/.../processor.py  -> cluster-free reference engine |
          +--------------------------------------------------------------+
                                       |
                                       v
          +--------------------------------------------------------------+
          | Exactly-once / idempotent sink  (src/.../sink.py)            |
          |  upsert by natural key (sat_id, window_start) + revision;    |
          |  committed-offset fence for replayed sources                 |
          +--------------------------------------------------------------+
                                       |
                                       v
          +--------------------------------------------------------------+
          | Apache Iceberg lakehouse (pyiceberg)                         |
          | table heliosnet.telemetry_windows, partitioned by sat_id     |
          | local filesystem warehouse by default; MinIO (S3) for scale  |
          +--------------------------------------------------------------+
                                       |
                                       v
          +--------------------------------------------------------------+
          | Grafana (provisioned dashboards, Infinity datasource)        |
          | anomaly counts, late-data folded, per-window drilldown       |
          +--------------------------------------------------------------+
```

## Two runtimes, one set of semantics

The windowing / watermark / late-data / exactly-once logic is the load-bearing
part of this repo, so it is implemented twice with the same behaviour:

- **`flink/anomaly_job.py`** is a real PyFlink job: `KafkaSource` ->
  `for_bounded_out_of_orderness` watermark -> `key_by(sat_id)` ->
  `TumblingEventTimeWindows` with `allowed_lateness` -> a `ProcessWindowFunction`
  that aggregates and classifies. It runs on the Flink cluster in
  `docker-compose.yml` (the `flink` profile).
- **`src/constellation_stream/processor.py`** is a small, dependency-free engine
  with the same semantics (event-time tumbling windows, watermark trailing the
  max event time by a fixed delay, allowed-lateness updates with revisions, a
  dead-letter list). It needs no JVM and no cluster, so it powers the unit tests,
  the CI scaled run, the benchmark, and the local demo.

This mirrors the honesty pattern of the rest of the Heliosnet suite: the heavy
distributed component is provided and runnable, and the in-environment numbers
come from the path that actually runs to completion here.

## Exactly-once sink

The source is treated as at-least-once (Kafka redelivery, recovery replays), and
the stream can re-emit a window when late data arrives. Exactly-once *effect* is
achieved without assuming an exactly-once source:

1. Every window result carries a natural key `(sat_id, window_start_ms)` and a
   monotonically increasing `revision`. The sink keeps one row per natural key
   and replaces it only on a strictly higher revision. Replays and stale updates
   are no-ops.
2. The Iceberg writer (`sink.commit_iceberg`) deletes any rows sharing a natural
   key with the batch before appending, in the same logical commit, so applying
   the same batch twice converges to the same table state.
3. A committed-offset fence (`apply_with_offset` / `commit`) ignores anything at
   or below the last durably committed offset, the way a transactional sink
   fences a resumed source.

## The path to billions

The committed headline number (see the README and `artifacts/BENCH.md`) is the
largest scale that completed in the build environment on a single core via the
reference engine. To reach billions of events:

- Run the **Flink** runtime, not the reference engine, with parallelism across a
  multi-node TaskManager pool (the job keys by `sat_id`, so it scales out by
  satellite with no cross-key state).
- Partition the Kafka topic by `sat_id` so each Flink subtask owns a satellite
  range; producer throughput scales with partitions and brokers.
- Land into Iceberg on object storage (MinIO/S3) with partitioned writes and
  periodic compaction; query through Trino/Spark for the dashboards.
- At parallelism `P` sustaining the per-core rate measured here, wall time for
  `N` events is roughly `N / (P x rate)`. The documented commands in the README
  reproduce the per-core rate; the rest is horizontal scale-out, which needs a
  multi-node cluster this single-machine build cannot bundle.
