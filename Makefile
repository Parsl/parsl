PYTHON := $(shell which python3 || echo ".python_is_missing")
SHELL := $(shell which bash) # Use bash instead of bin/sh as shell
GIT := $(shell which git || echo ".git_is_missing")
CWD := $(shell pwd)
DEPS := .deps
WORKQUEUE_INSTALL := /tmp/cctools
MPICH=mpich
OPENMPI=openmpi
EXECUTORS_PATH := $(shell ls -d parsl/executors/*/ | tr '\n' ':')
export PATH := $(EXECUTORS_PATH):$(WORKQUEUE_INSTALL)/bin/:$(PATH)
export CCTOOLS_VERSION=7.1.11
export HYDRA_LAUNCHER=fork
export OMPI_MCA_rmaps_base_oversubscribe=yes
MPI=$(MPICH)

.PHONY: help
help: ## me
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

VENV = .venv
.PHONY: virtualenv
virtualenv: ## create an activate a virtual env
	test -f venv/bin/activate || $(PYTHON) -m venv $(VENV)
	echo "Run 'source $(VENV)/bin/activate' to activate the virtual environment"


$(DEPS): test-requirements.txt requirements.txt
	pip3 install --upgrade pip
	pip3 install -r test-requirements.txt -r requirements.txt
	touch $(DEPS)

.PHONY: deps
deps: $(DEPS) ## install the dependencies

.PHONY: lint
lint: ## run linter script
	parsl/tests/lint-inits.sh

.PHONY: flake8
flake8:  ## run flake
	flake8 parsl/

.PHONY: clean_coverage
clean_coverage:
	rm -f .coverage

.PHONY: mypy
mypy: ## run mypy checks
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/tests/configs/
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/tests/test*/
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/tests/sites/
        # only the top level of monitoring is checked here because the visualization code does not type check
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/app/ parsl/channels/ parsl/dataflow/ parsl/data_provider/ parsl/launchers parsl/providers/ parsl/monitoring/*py
        # process worker pool is explicitly listed to check, because it is not
        # imported from anywhere in core parsl python code.
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/executors/high_throughput/process_worker_pool.py

.PHONY: local_thread_test
local_thread_test: ## run all tests with local_thread config
	pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/local_threads.py --cov=parsl --cov-append --cov-report= --random-order

.PHONY: htex_local_test
htex_local_test: ## run all tests with htex_local config
	PYTHONPATH=.  pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/htex_local.py --cov=parsl --cov-append --cov-report= --random-order

.PHONY: htex_local_alternate_test
htex_local_alternate_test: ## run all tests with htex_local config
	pip3 install ".[monitoring]"
	PYTHONPATH=.  pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/htex_local_alternate.py --cov=parsl --cov-append --cov-report= --random-order

$(WORKQUEUE_INSTALL):
	parsl/executors/workqueue/install-workqueue.sh

.PHONY: workqueue_ex_test
workqueue_ex_test: $(WORKQUEUE_INSTALL)  ## run all tests with workqueue_ex config
	PYTHONPATH=.:/tmp/cctools/lib/python3.5/site-packages  pytest parsl/tests/ -k "not cleannet and not issue363" --config parsl/tests/configs/workqueue_ex.py --cov=parsl --cov-append --cov-report= --random-order

.PHONY: workqueue_mon_test
workqueue_mon_test: $(WORKQUEUE_INSTALL)  ## run all tests with workqueue_ex config
	pip3 install ".[monitoring]"
	PYTHONPATH=.:/tmp/cctools/lib/python3.5/site-packages  pytest parsl/tests/ -k "not cleannet and not issue363" --config parsl/tests/configs/workqueue_monitoring_config.py --cov=parsl --cov-append --cov-report= --random-order


.PHONY: config_local_test
config_local_test: ## run all tests with workqueue_ex config
	echo "$(MPI)"
	parsl/executors/extreme_scale/install-mpi.sh $(MPI)
	pip3 install ".[extreme_scale,monitoring]"
	PYTHONPATH=.:/tmp/cctools/lib/python3.5/site-packages pytest parsl/tests/ -k "not cleannet" --config local --cov=parsl --cov-append --cov-report= --random-order

.PHONY: site_test
site_test:
	pytest parsl/tests/ -k "not cleannet" ${SHARED_FS_OPTIONS} --config parsl/tests/site_tests/site_config_selector.py --cov=parsl --cov-append --cov-report= --random-order
	pytest parsl/tests/site_tests/ ${SHARED_FS_OPTIONS} --config local

.PHONY: test ## run all tests with all config types
test: clean_coverage lint flake8 mypy local_thread_test htex_local_test htex_local_alternate_test workqueue_ex_test workqueue_mon_test ## run most tests

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

.PHONY: coverage
coverage: ## show the coverage report
	coverage report

.PHONY: clean
clean: ## clean up the environment by deleting the .venv, dist, eggs, mypy caches, coverage info, etc
	rm -rf .venv $(DEPS) dist *.egg-info .mypy_cache build .pytest_cache .coverage runinfo_* $(WORKQUEUE_INSTALL)
