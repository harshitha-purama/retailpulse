.PHONY: up down restart logs ps init generate-data spark-submit-streaming \
        dbt-run dbt-test train-models clean help

COMPOSE = docker compose
SPARK_MASTER = spark://spark-master:7077

# ──────────────────────────────────────────────────────────────────────────────
# CLUSTER LIFECYCLE
# ──────────────────────────────────────────────────────────────────────────────

## up: Start all services in detached mode
up:
	@echo "Starting RetailPulse platform..."
	$(COMPOSE) up -d
	@echo "Platform started. Run 'make ps' to check service status."

## down: Stop and remove containers (preserves volumes)
down:
	@echo "Stopping RetailPulse platform..."
	$(COMPOSE) down
	@echo "Platform stopped."

## restart: Full stop-and-start cycle
restart:
	@echo "Restarting RetailPulse platform..."
	$(COMPOSE) down
	$(COMPOSE) up -d
	@echo "Platform restarted."

## logs: Tail logs from all services (Ctrl+C to stop)
logs:
	$(COMPOSE) logs -f

## ps: Show status of all containers
ps:
	$(COMPOSE) ps

# ──────────────────────────────────────────────────────────────────────────────
# INITIALISATION
# ──────────────────────────────────────────────────────────────────────────────

## init: Bootstrap the full platform (Postgres schemas, MinIO buckets, Kafka topics, Airflow users)
init:
	@echo "==> [1/4] Waiting for Postgres to be healthy..."
	$(COMPOSE) run --rm -T postgres bash -c \
		"until pg_isready -h postgres -U retailpulse; do sleep 2; done"
	@echo "==> [2/4] Initialising Postgres databases..."
	$(COMPOSE) exec postgres psql -U retailpulse -f /docker-entrypoint-initdb.d/init.sql || true
	@echo "==> [3/4] Running MinIO bucket init..."
	$(COMPOSE) run --rm minio-init
	@echo "==> [4/4] Running Kafka topic init..."
	$(COMPOSE) run --rm kafka-init
	@echo "Init complete."

# ──────────────────────────────────────────────────────────────────────────────
# DATA GENERATION
# ──────────────────────────────────────────────────────────────────────────────

## generate-data: Run the synthetic data generator (one-shot)
generate-data:
	@echo "Running data generator..."
	$(COMPOSE) run --rm data-generator

# ──────────────────────────────────────────────────────────────────────────────
# SPARK STREAMING
# ──────────────────────────────────────────────────────────────────────────────

## spark-submit-streaming: Submit all Spark structured streaming jobs to the cluster
spark-submit-streaming:
	@echo "Submitting clickstream streaming job..."
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark/jobs/clickstream_streaming.py
	@echo "Submitting orders streaming job..."
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark/jobs/orders_streaming.py
	@echo "Submitting inventory streaming job..."
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark/jobs/inventory_streaming.py
	@echo "All streaming jobs submitted."

# ──────────────────────────────────────────────────────────────────────────────
# DBT
# ──────────────────────────────────────────────────────────────────────────────

## dbt-run: Run all dbt models
dbt-run:
	@echo "Running dbt models..."
	$(COMPOSE) run --rm -w /app/dbt airflow-scheduler \
		bash -c "pip install dbt-postgres --quiet && dbt run --profiles-dir /app/dbt --project-dir /app/dbt"

## dbt-test: Run all dbt data quality tests
dbt-test:
	@echo "Running dbt tests..."
	$(COMPOSE) run --rm -w /app/dbt airflow-scheduler \
		bash -c "pip install dbt-postgres --quiet && dbt test --profiles-dir /app/dbt --project-dir /app/dbt"

# ──────────────────────────────────────────────────────────────────────────────
# ML TRAINING
# ──────────────────────────────────────────────────────────────────────────────

## train-models: Trigger the ML training pipeline via Airflow REST API
train-models:
	@echo "Triggering ML training DAG via Airflow..."
	curl -s -X POST \
		"http://localhost:8080/api/v1/dags/ml_training_pipeline/dagRuns" \
		-H "Content-Type: application/json" \
		-u "admin:admin" \
		-d '{"conf": {"triggered_by": "make train-models"}}' | python3 -m json.tool
	@echo "ML training pipeline triggered. Monitor at http://localhost:8080"

# ──────────────────────────────────────────────────────────────────────────────
# CLEANUP
# ──────────────────────────────────────────────────────────────────────────────

## clean: Stop all containers AND remove all named volumes (destructive!)
clean:
	@echo "WARNING: This will delete all persistent data (volumes)."
	@echo "Press Ctrl+C within 5 seconds to abort..."
	@sleep 5
	$(COMPOSE) down -v
	@echo "All containers and volumes removed."

# ──────────────────────────────────────────────────────────────────────────────
# HELP
# ──────────────────────────────────────────────────────────────────────────────

## help: Print this help message
help:
	@echo ""
	@echo "RetailPulse - Available make targets:"
	@echo "──────────────────────────────────────────────────────────────────"
	@grep -E '^##' Makefile | sed 's/## /  /' | column -t -s ':'
	@echo ""
	@echo "Service URLs (after 'make up'):"
	@echo "  Airflow Webserver : http://localhost:8080  (admin / admin)"
	@echo "  MLflow            : http://localhost:5000"
	@echo "  FastAPI           : http://localhost:8000"
	@echo "  Apache Superset   : http://localhost:8088  (admin / admin)"
	@echo "  JupyterLab        : http://localhost:8888  (token: retailpulse)"
	@echo "  Trino             : http://localhost:8085"
	@echo "  Spark Master UI   : http://localhost:8090"
	@echo "  Grafana           : http://localhost:3000  (admin / admin)"
	@echo "  Prometheus        : http://localhost:9090"
	@echo "  MinIO Console     : http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "  Kafka             : localhost:29092        (external)"
	@echo "  PostgreSQL        : localhost:5432         (retailpulse / retailpulse123)"
	@echo "──────────────────────────────────────────────────────────────────"
	@echo ""
