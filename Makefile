SHELL := $(shell which bash) # Use bash instead of bin/sh as shell
SYS_PYTHON := $(shell which python3 || echo ".python_is_missing")
GIT := $(shell which git || echo ".git_is_missing")
CWD := $(shell pwd)
VENV = .venv
VENV_BIN := $(VENV)/bin
PIP := $(VENV_BIN)/pip3
MYPY := $(VENV_BIN)/mypy
DEPS := $(VENV)/.deps
PYTHON := $(VENV_BIN)/python3
PYTEST := $(VENV_BIN)/pytest
WORKQUEUE_INSTALL := /tmp/cctools
MPICH=mpich
OPENMPI=openmpi
EXECUTORS_PATH := $(shell ls -d parsl/executors/*/ | tr '\n' ':')
export PATH := $(EXECUTORS_PATH):$(WORKQUEUE_INSTALL)/bin/:$(VENV_BIN):$(PATH)
export CCTOOLS_VERSION=7.0.11
export HYDRA_LAUNCHER=fork
export OMPI_MCA_rmaps_base_oversubscribe=yes
export MPI=$(MPICH)

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

.PHONY: flake8
flake8:  ## run flake
	$(VENV_BIN)/flake8 parsl/

.PHONY: clean_coverage
clean_coverage:
	rm -f .coverage

.PHONY: mypy
mypy: ## run mypy checks
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/tests/configs/
	MYPYPATH=$(CWD)/mypy-stubs $(MYPY) parsl/tests/test*/
	MYPYPATH=$(CWD)/mypy-stubs $(MYPY) parsl/tests/sites/
	MYPYPATH=$(CWD)/mypy-stubs $(MYPY) parsl/app/ parsl/channels/ parsl/dataflow/ parsl/data_provider/ parsl/launchers parsl/providers/

.PHONY: local_thread_test
local_thread_test: $(DEPS) ## run all tests with local_thread config
	$(PYTEST) parsl -k "not cleannet" --config parsl/tests/configs/local_threads.py --cov=parsl --cov-append --cov-report= --random-order

.PHONY: htex_local_test
htex_local_test: $(DEPS) ## run all tests with htex_local config
	PYTHONPATH=.  $(PYTEST) parsl -k "not cleannet" --config parsl/tests/configs/htex_local.py --cov=parsl --cov-append --cov-report= --random-order

.PHONY: htex_local_alternate_test
htex_local_alternate_test: $(DEPS) ## run all tests with htex_local config
	parsl/executors/extreme_scale/install-mpi.sh $(MPI)
	$(PIP) install ".[extreme_scale,monitoring]"
	PYTHONPATH=.  $(PYTEST) parsl -k "not cleannet" --config parsl/tests/configs/htex_local_alternate.py --cov=parsl --cov-append --cov-report= --random-order

$(WORKQUEUE_INSTALL):
	parsl/executors/workqueue/install-workqueue.sh

.PHONY: workqueue_ex_test
workqueue_ex_test: $(DEPS) $(WORKQUEUE_INSTALL)  ## run all tests with workqueue_ex config
	$(PIP) install ".[extreme_scale]"
	work_queue_worker localhost 9000  &> /dev/null &
	PYTHONPATH=.:/tmp/cctools/lib/python3.5/site-packages  $(PYTEST) parsl -k "not cleannet" --config parsl/tests/configs/workqueue_ex.py --cov=parsl --cov-append --cov-report= --random-order --bodge-dfk-per-test
	kill -3 $(ps aux | grep -E -e "[0-9]+:[0-9]+ work_queue_worker" | tr -s ' ' | cut -f 2 -d " ")

.PHONY: config_local_test
config_local_test: $(DEPS) ## run all tests with workqueue_ex config
	$(PIP) install ".[extreme_scale]"
	PYTHONPATH=. $(PYTEST) parsl -k "not cleannet" --config local --cov=parsl --cov-append --cov-report= --random-order

.PHONY: test ## run all tests with all config types
test: $(DEPS) clean_coverage lint flake8 local_thread_test htex_local_test config_local_test htex_local_alternate_test workqueue_ex_test  ## run all tests

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
clean: ## clean up the environment by deleting the .venv, dist, eggs, mypy caches, coverage info, etc
	rm -rf $(VENV) dist *.egg-info .mypy_cache build .pytest_cache .coverage runinfo_* $(WORKQUEUE_INSTALL)
