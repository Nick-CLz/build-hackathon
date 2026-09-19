# Insurance lakehouse skeleton.
# Every target is documented inline; `make help` renders them.
SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV    := .venv
PY      := $(VENV)/bin/python
PIP     := $(VENV)/bin/uv pip
PYTEST  := $(VENV)/bin/pytest
RUFF    := $(VENV)/bin/ruff

# Overridable on the command line: `make data DAY=2 N=5000`
DAY ?= 1
N   ?=
export LAKEHOUSE_N_POLICIES := $(if $(N),$(N),$(LAKEHOUSE_N_POLICIES))

# Spark 3.5 supports Java 8/11/17 only. Search for a 17 JDK across macOS and
# Linux and PREFER it over an inherited JAVA_HOME, which on many machines (and
# in this project's own CI image) points at a newer JDK that Spark 3.5 rejects.
JAVA17_HOME := $(shell \
  if [ -x /usr/libexec/java_home ]; then /usr/libexec/java_home -v 17 2>/dev/null; \
  elif [ -d /usr/lib/jvm/java-17-openjdk-amd64 ]; then echo /usr/lib/jvm/java-17-openjdk-amd64; \
  else ls -d /usr/lib/jvm/*17* 2>/dev/null | head -1; fi)
JAVA_HOME := $(if $(JAVA17_HOME),$(JAVA17_HOME),$(JAVA_HOME))
export JAVA_HOME
export SPARK_CONF_DIR := $(CURDIR)/conf
# The proxy-injected JAVA_TOOL_OPTIONS breaks spark-submit arg parsing in CI.
unexport JAVA_TOOL_OPTIONS

.PHONY: help
help: ## Show this help
	@echo "Insurance lakehouse skeleton"
	@echo ""
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS=":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Variables: DAY=<n> (daily drop)  N=<policies>  TARGET=local|databricks"

# ---------------------------------------------------------------- setup
.PHONY: setup
setup: ## Create the venv, install deps, verify the Java version
	@command -v uv >/dev/null || { echo "install uv: https://docs.astral.sh/uv/"; exit 1; }
	uv venv --python 3.11 $(VENV)
	VIRTUAL_ENV=$(CURDIR)/$(VENV) uv pip install -e ".[dev]"
	@$(MAKE) --no-print-directory check-java
	@echo "setup complete -- next: make data"

.PHONY: check-java
check-java: ## Verify a Java 17 JDK is resolvable (Spark 3.5 requires 8/11/17)
	@if [ -z "$(JAVA_HOME)" ] || [ ! -x "$(JAVA_HOME)/bin/java" ]; then \
	  echo "ERROR: no Java 17 found. macOS: brew install openjdk@17"; \
	  echo "       Debian/Ubuntu: sudo apt install openjdk-17-jdk-headless"; exit 1; fi
	@v=$$("$(JAVA_HOME)/bin/java" -version 2>&1 | head -1); \
	  case "$$v" in *\"17.*) echo "java ok: $$v";; \
	  *) echo "ERROR: need Java 17, found: $$v"; exit 1;; esac

# ---------------------------------------------------------------- data
.PHONY: data
data: ## Generate daily drop DAY (default 1). e.g. make data DAY=2
	$(PY) -m data_generator --day $(DAY)

.PHONY: defects
defects: ## List the deliberate data defects the generator injects
	$(PY) -m data_generator --list-defects

# ---------------------------------------------------------------- quality
.PHONY: test
test: ## Run the pytest suite
	$(PYTEST)

.PHONY: lint
lint: ## ruff + yamllint (sqlfluff added with the dbt project)
	$(RUFF) check .
	$(RUFF) format --check .
	$(VENV)/bin/yamllint -c .yamllint.yml . || true

.PHONY: format
format: ## Auto-fix formatting and simple lint errors
	$(RUFF) check --fix .
	$(RUFF) format .

.PHONY: docs-sync
docs-sync: ## Regenerate generated tables in docs/
	$(PY) scripts/sync_doc_tables.py

.PHONY: dbt-sources
dbt-sources: ## Regenerate dbt/models/sources.yml from config/sources.yml
	$(PY) scripts/generate_dbt_sources.py

.PHONY: generate
generate: docs-sync dbt-sources ## Regenerate every generated file

# ---------------------------------------------------------------- cleanup
.PHONY: clean
clean: ## Remove generated data and caches (keeps the venv)
	rm -rf lakehouse reports dbt/target dbt/logs .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true

.PHONY: reset
reset: clean ## clean, plus drop the venv and dbt packages
	rm -rf $(VENV) dbt/dbt_packages metastore_db derby.log

# ---------------------------------------------------------------- bronze
.PHONY: bronze
bronze: ## Ingest the landing zone into bronze Delta tables (idempotent)
	$(PY) -m ingestion

.PHONY: sources
sources: ## List the registered sources from config/sources.yml
	$(PY) -m ingestion --list-sources

.PHONY: maintenance
maintenance: ## Probe Delta OPTIMIZE/ZORDER/CLUSTER BY/VACUUM/time travel support
	$(PY) scripts/delta_maintenance.py --source $(or $(SOURCE),partner_policies_a)

.PHONY: inspect
inspect: ## Show bronze tables, manifest, schema drift and quarantine
	$(PY) scripts/inspect_bronze.py

# ---------------------------------------------------------------- docker
COMPOSE := docker compose -f docker/docker-compose.yml

.PHONY: build
build: ## Build the Spark + Delta + dbt Docker image
	$(COMPOSE) build

.PHONY: up
up: ## Start the container in the background
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop and remove the container
	$(COMPOSE) down

.PHONY: shell
shell: ## Open a shell inside the container
	$(COMPOSE) run --rm lakehouse bash

# ---------------------------------------------------------------- dbt
# dbt is ALWAYS invoked from the project root. conf/spark-defaults.conf uses
# relative paths (it cannot expand env vars), so running dbt from inside dbt/
# would point Spark at a second, empty metastore.
DBT := $(VENV)/bin/dbt --no-use-colors
DBT_DIRS := --project-dir dbt --profiles-dir dbt
export DBT_TARGET ?= local

.PHONY: dbt-deps
dbt-deps: ## Install dbt packages
	$(DBT) deps $(DBT_DIRS)

.PHONY: dbt-build
dbt-build: ## Run models, tests, snapshots and seeds (dbt build)
	$(DBT) build $(DBT_DIRS) --target $(DBT_TARGET)

.PHONY: dbt-run
dbt-run: ## Run models only
	$(DBT) run $(DBT_DIRS) --target $(DBT_TARGET)

.PHONY: dbt-test
dbt-test: ## Run dbt tests only
	$(DBT) test $(DBT_DIRS) --target $(DBT_TARGET)

.PHONY: snapshot
snapshot: ## Run dbt snapshots (SCD2 policy history)
	$(DBT) snapshot $(DBT_DIRS) --target $(DBT_TARGET)

.PHONY: freshness
freshness: ## Check source freshness against the SLAs in config/sources.yml
	$(DBT) source freshness $(DBT_DIRS) --target $(DBT_TARGET)

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs
	$(DBT) docs generate $(DBT_DIRS) --target $(DBT_TARGET)
	$(DBT) docs serve $(DBT_DIRS) --port 8081

.PHONY: dbt-test-singular
dbt-test-singular: ## Run only the singular (hand-written) dbt tests
	$(DBT) test $(DBT_DIRS) --target $(DBT_TARGET) -s "test_type:singular"

.PHONY: report
report: ## Parse dbt artifacts into Delta + render reports/observability.html
	$(PY) scripts/build_observability_report.py

.PHONY: dbt-manifest
dbt-manifest: ## Build dbt manifest.json (Cosmos loads the DAG from this)
	$(DBT) parse $(DBT_DIRS) --target $(DBT_TARGET)

# ---------------------------------------------------------------- airflow
AIRFLOW_VENV := .venv-airflow
AIRFLOW_LOCAL_ENV := AIRFLOW_HOME=$(CURDIR)/lakehouse/airflow_home \
  AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/airflow/dags \
  AIRFLOW__CORE__LOAD_EXAMPLES=False \
  LAKEHOUSE_REPO=$(CURDIR) DBT_EXECUTABLE=$(CURDIR)/$(VENV)/bin/dbt \
  PATH="$(CURDIR)/$(VENV)/bin:$$PATH"

.PHONY: airflow-setup
airflow-setup: ## Install Airflow into a separate venv for local DAG runs
	uv venv --python 3.11 $(AIRFLOW_VENV)
	VIRTUAL_ENV=$(CURDIR)/$(AIRFLOW_VENV) uv pip install \
	  "apache-airflow==3.3.2" "apache-airflow-providers-standard" "astronomer-cosmos==1.15.1"

.PHONY: airflow-test
airflow-test: ## Execute the DAG end-to-end locally (no containers needed)
	@mkdir -p lakehouse/airflow_home
	$(AIRFLOW_LOCAL_ENV) $(AIRFLOW_VENV)/bin/airflow db migrate >/dev/null
	$(AIRFLOW_LOCAL_ENV) $(AIRFLOW_VENV)/bin/airflow dags reserialize >/dev/null
	$(AIRFLOW_LOCAL_ENV) $(AIRFLOW_VENV)/bin/airflow dags test insurance_lakehouse $(or $(DATE),2026-09-19)

.PHONY: airflow-up
airflow-up: ## Start Airflow via the Astro CLI (webserver on :8080)
	cd airflow && astro dev start

.PHONY: airflow-down
airflow-down: ## Stop the Astro environment
	cd airflow && astro dev stop

# ---------------------------------------------------------------- pipeline
.PHONY: all
all: ## Full pipeline from clean: data -> bronze -> dbt build -> report
	$(MAKE) data DAY=1
	$(MAKE) bronze
	$(MAKE) dbt-deps
	$(MAKE) dbt-build
	$(MAKE) report
	@echo ""
	@echo "Pipeline complete. Report: reports/observability.html"

.PHONY: demo
demo: ## Day 1 -> day 2, then print proof of every hard case
	@echo "########## DAY 1: initial load ##########"
	$(MAKE) --no-print-directory data DAY=1
	$(MAKE) --no-print-directory bronze
	$(MAKE) --no-print-directory dbt-build
	@echo ""
	@echo "########## DAY 2: updates, duplicates, late claims, schema drift ##########"
	$(MAKE) --no-print-directory data DAY=2
	$(MAKE) --no-print-directory bronze
	$(MAKE) --no-print-directory dbt-build
	$(MAKE) --no-print-directory report
	@echo ""
	@echo "########## PROOF ##########"
	$(PY) scripts/demo_proof.py

# ---------------------------------------------------------------- ci
.PHONY: ci
ci: ## Run every CI step locally, in the same order as the workflow
	@echo "===== 1/4 lint ====="
	$(RUFF) check .
	$(RUFF) format --check .
	$(VENV)/bin/yamllint -c .yamllint.yml .
	$(VENV)/bin/sqlfluff lint dbt/
	@echo "===== 2/4 pytest ====="
	$(PYTEST) tests/ -m "not slow" -q
	$(PYTEST) tests/ -m slow -q
	@echo "===== 3/4 end-to-end pipeline ====="
	$(MAKE) --no-print-directory clean
	$(MAKE) --no-print-directory dbt-deps
	$(MAKE) --no-print-directory data DAY=1
	$(MAKE) --no-print-directory bronze
	@$(MAKE) --no-print-directory bronze | tee /tmp/lakehouse_second_ingest.log >/dev/null; \
	  grep -q UP_TO_DATE /tmp/lakehouse_second_ingest.log \
	    || { echo "FAIL: re-ingest was not idempotent"; exit 1; }
	@echo "  idempotency: second ingest reported UP_TO_DATE"
	$(MAKE) --no-print-directory dbt-build
	$(MAKE) --no-print-directory data DAY=2
	$(MAKE) --no-print-directory bronze
	$(MAKE) --no-print-directory dbt-build
	$(MAKE) --no-print-directory report
	$(PY) scripts/demo_proof.py
	@echo "===== 4/4 airflow DAG integrity ====="
	AIRFLOW_HOME=$(CURDIR)/lakehouse/airflow_home LAKEHOUSE_REPO=$(CURDIR) \
	  AIRFLOW__CORE__LOAD_EXAMPLES=False \
	  $(AIRFLOW_VENV)/bin/python -m pytest airflow/tests/dags/ -q
	@mkdir -p lakehouse/airflow_home
	@$(AIRFLOW_LOCAL_ENV) $(AIRFLOW_VENV)/bin/airflow db migrate >/dev/null 2>&1
	@$(AIRFLOW_LOCAL_ENV) $(AIRFLOW_VENV)/bin/airflow dags reserialize >/dev/null 2>&1
	@$(AIRFLOW_LOCAL_ENV) $(AIRFLOW_VENV)/bin/airflow dags list-import-errors 2>&1 \
	  | tee /tmp/lakehouse_dag_errs.log | grep -q "No data found" \
	  && echo "  DAG imports cleanly" \
	  || { echo "FAIL: DAG import errors"; cat /tmp/lakehouse_dag_errs.log; exit 1; }
	@echo ""
	@echo "===== CI PASSED ====="

.PHONY: governance
governance: ## Generate governance/unity_catalog.sql from PII metadata
	$(PY) governance/generate_unity_catalog.py --catalog $(or $(CATALOG),insurance_dev)
