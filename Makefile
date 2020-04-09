SHELL := $(shell which bash) # Use bash instead of bin/sh as shell
SYS_PYTHON := $(shell which python3.7 || echo ".python_is_missing")
GIT := $(shell which git || echo ".git_is_missing")
VENV = .venv
VENV_BIN := $(VENV)/bin
PIP := $(VENV_BIN)/pip
DEPS := $(VENV)/.deps
PYTHON := $(VENV_BIN)/python3
PYTEST := $(VENV_BIN)/pytest
export PATH := $(VENV_BIN):$(PATH)

.PHONY: help
help: ## me
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

$(VENV):
	$(SYS_PYTHON) -m venv $(VENV)

$(DEPS): $(VENV) test-requirements.txt
	$(PIP) install --upgrade pip
	$(PIP) install -r test-requirements.txt
	touch $(DEPS)

.PHONY: deps
deps: $(DEPS) ## install the dependencies

.PHONY:
lint: ## run linter script
	parsl/tests/lint-inits.sh

.PHONY: flake
flake:  ## run flake
	$(VENV_BIN)/flake8 parsl/


.PHONY: clean_coverage
clean_coverage:
	rm -f .coverage


.PHONY: test
test: $(DEPS) clean_coverage lint flake local_thread_test ## run all tests


.PHONY: local_thread_test
local_thread_test: $(DEPS) clean_coverage lint flake ## run all tests with local_thread config
	$(PYTEST) parsl -k "not cleannet" --config parsl/tests/configs/local_threads.py --cov=parsl --cov-append --cov-report= --random-order


.PHONY: local_thread_test
htex_local_test: $(DEPS) clean_coverage lint flake ## run all tests with htex_local config
	$(PYTEST) parsl -k "not cleannet" --config parsl/tests/configs/htex_local.py --cov=parsl --cov-append --cov-report= --random-order


.PHONY: tag
tag: ## create a tag in git. to run, do a 'make VERSION="version string" tag
	./tag_and_release.sh create_tag $(VERSION)


.PHONY: package
package: ## package up a distribution.
	./tag_and_release.sh package


.PHONY: deploy
deploy: ## deploy the distribution
	./tag_and_release.sh release


.PHONY: release
release: deps tag package deploy   ## create a release. To run, do a 'make VERSION="version string"  release'


coverage: test ## show the coverage report
	$(VENV_BIN)/coverage report


.PHONY: clean
clean: ## remove venv and flush out
	rm -rf $(VENV) dist *.egg-info .mypy_cache
	find . -name __pycache__ | xargs --no-run-if-empty rm -rf
