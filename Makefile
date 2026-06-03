SHELL := /bin/bash
COMPOSE := docker compose -f infra/docker/docker-compose.yml
PYTHON := python

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

.PHONY: local-up
local-up: ## Start local platform
	$(COMPOSE) up -d

.PHONY: local-down
local-down: ## Stop local platform
	$(COMPOSE) down -v

.PHONY: topics
topics: ## Create Kafka topics
	bash scripts/create_topics.sh

.PHONY: produce-events
produce-events: ## Produce synthetic KYC events
	$(PYTHON) ingestion/producers/kyc_event_producer.py --events 250 --sleep-ms 10

.PHONY: test
test: ## Run tests
	pytest -q tests/test_event_factory.py

.PHONY: test-spark
test-spark: ## Run Spark tests
	pytest -q tests/test_spark_transformations.py

.PHONY: lint
lint: ## Run ruff checks
	ruff check ingestion spark_jobs tests

.PHONY: format
format: ## Format Python files
	ruff format ingestion spark_jobs tests

.PHONY: dbt-build-local
dbt-build-local: ## Run dbt locally with DuckDB
	cd dbt_kyc && DBT_PROFILES_DIR=. dbt deps && DBT_PROFILES_DIR=. dbt seed && DBT_PROFILES_DIR=. dbt build --target local
