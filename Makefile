.PHONY: help install install-lake test bench bench-big pipeline export up down demo flink-up flink-submit produce clean

PY ?= python
COMPOSE ?= docker compose

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install the package with dev extras (no JVM, no cluster)
	$(PY) -m pip install -e ".[dev]"

install-lake: ## Install the Apache Iceberg lakehouse extras (pyiceberg + pyarrow)
	$(PY) -m pip install -e ".[dev,lake]"

test: ## Run unit tests + quality gate (windowing, late-data, exactly-once; coverage floor)
	$(PY) -m pytest --cov=constellation_stream --cov-report=term-missing --cov-fail-under=85

bench: ## Real benchmark at the CI scale (writes artifacts/bench.json + BENCH.md)
	$(PY) -m constellation_stream.bench --rows 1000000 --query-iters 300 --outdir artifacts

bench-big: ## Real benchmark at the documented headline scale (~10M rows)
	$(PY) -m constellation_stream.bench --rows 10000000 --query-iters 300 --outdir artifacts

pipeline: ## Run the local pipeline into the Apache Iceberg lake (needs install-lake)
	$(PY) -m constellation_stream.pipeline

export: ## Export window results to grafana/data/windows.json for the dashboards
	$(PY) -m constellation_stream.export

up: ## Start the ingest + lake + Grafana plane (Redpanda + MinIO + Grafana)
	$(COMPOSE) up -d --wait redpanda minio
	$(COMPOSE) up -d grafana

down: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

demo: ## One command: start the plane, export window data, open Grafana
	$(COMPOSE) up -d --wait redpanda minio
	$(COMPOSE) up -d grafana
	$(PY) -m constellation_stream.export
	@echo ""
	@echo "  Grafana      : http://localhost:3000  (heliosnet / heliosnet)"
	@echo "  MinIO console: http://localhost:9001   (heliosnet / heliosnet123)"
	@echo "  Redpanda     : localhost:19092"
	@echo ""

flink-up: ## Start the real Apache Flink cluster (jobmanager + taskmanager)
	$(COMPOSE) --profile flink up -d --wait redpanda
	$(COMPOSE) --profile flink up -d flink-jobmanager flink-taskmanager
	@echo "  Flink UI: http://localhost:8081"

flink-submit: ## Submit the PyFlink anomaly job to the running cluster
	$(COMPOSE) exec flink-jobmanager flink run -py /opt/flink/usrlib/anomaly_job.py

produce: ## Stream generated telemetry into Redpanda (needs the kafka extra)
	$(PY) -m constellation_stream.produce

clean: ## Remove build/test/run artifacts
	rm -rf artifacts .pytest_cache .coverage htmlcov src/*.egg-info warehouse grafana/data
