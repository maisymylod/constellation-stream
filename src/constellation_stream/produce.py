"""Stream generated telemetry into Kafka/Redpanda.

Produces one JSON message per telemetry record to the telemetry topic, keyed by
satellite id so a partition holds a satellite's ordered history (modulo the
deliberately-injected late records). Bounded by :class:`GenConfig` so the demo
terminates deterministically.

    python -m constellation_stream.produce
"""
from __future__ import annotations

import json

from .config import GenConfig, kafka_bootstrap, telemetry_topic
from .generator import stream


def main(argv: list[str] | None = None) -> int:
    from kafka import KafkaProducer  # lazy: benchmark/tests need no kafka

    cfg = GenConfig()
    topic = telemetry_topic()
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap(),
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        linger_ms=20,
        acks="all",  # exactly-once-friendly: wait for full ISR ack
        enable_idempotence=True,
    )
    n = 0
    for rec in stream(cfg):
        producer.send(topic, key=rec["sat_id"], value=rec)
        n += 1
    producer.flush()
    producer.close()
    print(f"produced {n} telemetry records to topic '{topic}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
