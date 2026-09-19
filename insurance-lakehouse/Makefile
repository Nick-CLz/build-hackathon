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

# ---------------------------------------------------------------- cleanup
.PHONY: clean
clean: ## Remove generated data and caches (keeps the venv)
	rm -rf lakehouse reports dbt/target dbt/logs .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true

.PHONY: reset
reset: clean ## clean, plus drop the venv and dbt packages
	rm -rf $(VENV) dbt/dbt_packages metastore_db derby.log
