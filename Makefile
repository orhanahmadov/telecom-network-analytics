COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
PYTHON  ?= .venv/bin/python
COUNT ?= 1000
BATCH_SIZE ?= 200

.DEFAULT_GOAL := help
.PHONY: help init-env venv up down clean ps logs register-connector generate generate-forever consume smoke test dbt-debug lint format format-check compile-check compose-validate ci

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# `.env` is a real file target: `make up` creates it automatically (with random secrets) if missing.
.env:
	python3 scripts/init_env.py

init-env: .env ## Create .env from .env.example with freshly generated secrets

venv: ## Create .venv and install pinned dev dependencies
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt

up: .env ## Build and start the full infrastructure in the background
	$(COMPOSE) up -d --build
	$(COMPOSE) ps

down: ## Stop the stack, keep data (named volumes persist)
	$(COMPOSE) down

clean: ## Full reset: stop the stack and DELETE all volumes (all data lost)
	$(COMPOSE) down -v

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Tail logs from all services (Ctrl+C to stop)
	$(COMPOSE) logs -f

register-connector: ## Register/update the Debezium CDC connector (idempotent; stack must be up)
	$(PYTHON) infra/kafka-connect/register_connector.py

generate: ## Write COUNT synthetic events into source Postgres (make generate COUNT=5000)
	PYTHONPATH=. $(PYTHON) -m ingestion.producer.db_writer --count $(COUNT)

generate-forever: ## Keep writing events until Ctrl+C
	PYTHONPATH=. $(PYTHON) -m ingestion.producer.db_writer --forever

consume: ## Consume the Kafka topic into RustFS batches (Ctrl+C to stop; make consume BATCH_SIZE=50)
	PYTHONPATH=. $(PYTHON) -m ingestion.consumer.kafka_to_rustfs --batch-size $(BATCH_SIZE)

smoke: ## Check the pipeline is wired: connector RUNNING, databases, object store, topic (stack must be up)
	PYTHONPATH=. $(PYTHON) -m scripts.smoke_check

test: ## Run the test suite
	PYTHONPATH=. $(PYTHON) -m pytest -q

lint: ## Check code with ruff (no changes made)
	$(PYTHON) -m ruff check .

format: ## Auto-format code with ruff (rewrites files)
	$(PYTHON) -m ruff format .

format-check: ## Check formatting with ruff (no changes made; used by CI)
	$(PYTHON) -m ruff format --check .

compile-check: ## Byte-compile every pipeline module to catch syntax/import errors
	$(PYTHON) -m compileall -q config ingestion orchestration scripts transformation tests

compose-validate: ## Validate infra/docker-compose.yml (needs Docker; generates a throwaway .env if missing)
	@test -f .env || python3 scripts/init_env.py
	docker compose -f infra/docker-compose.yml --env-file .env config -q

ci: lint format-check compile-check test ## Run everything CI runs, except compose-validate (needs Docker)

dbt-debug: ## Check dbt can connect to the warehouse, from inside the Airflow container
	$(COMPOSE) exec airflow-scheduler bash -c "cd /opt/airflow/project/transformation/dbt_telecom && DBT_PROFILES_DIR=. /home/airflow/dbt-venv/bin/dbt debug --connection"
