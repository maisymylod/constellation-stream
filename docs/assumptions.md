# Assumptions, trust boundaries, and known limitations

This repo is honest about what is real, what is simulated, and what is provided
but not exercised in the build environment.

## Simulated

- **The telemetry.** There is no physical constellation. `generator.py`
  synthesises labelled telemetry over the Heliosnet channel schema (vendored from
  the `constellation` sibling) with a diurnal/orbital component, per-satellite
  offsets, and injectable ground-truth anomalies (drift / dropout /
  thermal_runaway). It also deliberately delays a fraction of records so the
  late-data path is exercised by real out-of-order input, not a mock.

## Real (runs in this environment, exercised by CI)

- **The stream-processing semantics.** Event-time tumbling windows, watermarks,
  allowed-lateness updates with revisions, and dead-lettering are implemented in
  `processor.py` and unit-tested deterministically (`tests/test_windowing.py`,
  `tests/test_late_data.py`).
- **The exactly-once / idempotent sink.** Upsert-by-natural-key + revision and
  the committed-offset fence are unit-tested, including a full-replay idempotency
  test (`tests/test_exactly_once.py`).
- **The Apache Iceberg lakehouse path.** The local pipeline writes window results
  into a real Iceberg table (pyiceberg) and re-running is idempotent; CI runs the
  pipeline twice and asserts the table is unchanged.
- **The benchmark numbers.** `bench.py` measures real ingest/processing
  throughput and real query p50/p95/p99 over the materialised results and writes
  them to `artifacts/bench.json` + `artifacts/BENCH.md`. Nothing is hand-written.

## Provided and runnable, but NOT the source of the headline

- **The real Apache Flink job** (`flink/anomaly_job.py`) runs on the Flink
  cluster in `docker-compose.yml` (the `flink` profile). It is the same semantics
  at cluster fidelity. It is not the headline because the build environment runs
  Python 3.14 (no PyFlink wheel) and a full Flink + Kafka cluster is heavier than
  the CI budget. The in-environment headline comes from the reference engine.
- **MinIO (S3) warehouse and Grafana-on-cluster.** The default warehouse is the
  local filesystem so the pipeline runs with zero infrastructure; `docker-compose`
  provides MinIO and Grafana for the fuller setup.

## Stream-processor choice

The headline pipeline uses a **purpose-built, cluster-free reference engine**
(pure Python, single core) rather than Spark or Flink for the in-environment run.
Reasons, stated plainly:

- PyFlink has no wheel for Python 3.14 (the build environment's interpreter).
- Spark 4.x requires Java 17+; only Java 11 is available here.

The reference engine implements the same event-time semantics and is what makes
the logic deterministically unit-testable in CI without a cluster. The real Flink
job is committed alongside it for cluster-scale runs. This is the same
"documented-but-not-run-here" honesty the `groundstation-train` sibling applies to
its GPU training step.

## Determinism

Everything that affects output is seeded (`GenConfig.seed = 42`, per-satellite
`SeedSequence`). The generator, processor, sink, and benchmark are deterministic;
`make bench` reproduces the same throughput shape and identical window counts on
the same hardware.
