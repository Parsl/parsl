PYTHON := $(shell which python3 || echo ".python_is_missing")
SHELL := $(shell which bash) # Use bash instead of bin/sh as shell
GIT := $(shell which git || echo ".git_is_missing")
CWD := $(shell pwd)
DEPS := .deps
CCTOOLS_INSTALL := /tmp/cctools
MPICH=mpich
OPENMPI=openmpi
export PATH := $(CCTOOLS_INSTALL)/bin/:$(PATH)
export CCTOOLS_VERSION=7.8.0
export HYDRA_LAUNCHER=fork
export OMPI_MCA_rmaps_base_oversubscribe=yes
MPI=$(MPICH)

.PHONY: help
help: ## me
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

VENV = .venv
.PHONY: virtualenv
virtualenv: ## create an activate a virtual env
	test -f $(VENV)/bin/activate || $(PYTHON) -m venv $(VENV)
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

.PHONY: isort
isort: ## run isort on all files
	isort --check .

.PHONY: flake8
flake8:  ## run flake
	flake8 parsl/

.PHONY: clean_coverage
clean_coverage:
	rm -f .coverage

.PHONY: mypy
mypy: ## run mypy checks
	MYPYPATH=$(CWD)/mypy-stubs mypy parsl/

.PHONY: local_thread_test
local_thread_test: ## run all tests with local_thread config
	pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/local_threads.py --random-order --durations 10

.PHONY: htex_local_test
htex_local_test: ## run all tests with htex_local config
	pip3 install .
	pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/htex_local.py --random-order --durations 10

.PHONY: htex_local_alternate_test
htex_local_alternate_test: ## run all tests with htex_local config
	pip3 install ".[monitoring]"
	pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/htex_local_alternate.py --random-order --durations 10

$(CCTOOLS_INSTALL):	#CCtools contains both taskvine and workqueue so install only once
	parsl/executors/taskvine/install-taskvine.sh

.PHONY: vineex_local_test
vineex_local_test: $(CCTOOLS_INSTALL)  ## run all tests with taskvine_ex config
	PYTHONPATH=/tmp/cctools/lib/python3.8/site-packages  pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/taskvine_ex.py --random-order --durations 10

.PHONY: wqex_local_test
wqex_local_test: $(CCTOOLS_INSTALL)  ## run all tests with workqueue_ex config
	PYTHONPATH=/tmp/cctools/lib/python3.8/site-packages  pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/workqueue_ex.py --random-order --durations 10

.PHONY: radical_local_test
radical_local_test:
	pip3 install ".[radical-pilot]"
	mkdir -p ~/.radical/pilot/configs && echo '{"localhost": {"virtenv_mode": "local"}}' > ~/.radical/pilot/configs/resource_local.json
	pytest parsl/tests/ -k "not cleannet and not issue3328 and not executor_supports_std_stream_tuples" --config parsl/tests/configs/local_radical.py --random-order --durations 10

.PHONY: config_local_test
config_local_test: $(CCTOOLS_INSTALL)
	pip3 install ".[monitoring,visualization,proxystore]"
	PYTHONPATH=/tmp/cctools/lib/python3.8/site-packages pytest parsl/tests/ -k "not cleannet" --config local --random-order --durations 10

.PHONY: site_test
site_test:
	pytest parsl/tests/ -k "not cleannet" ${SHARED_FS_OPTIONS} --config parsl/tests/site_tests/site_config_selector.py --random-order
	pytest parsl/tests/site_tests/ ${SHARED_FS_OPTIONS} --config local

.PHONY: perf_test
perf_test:
	parsl-perf --time 5 --config parsl/tests/configs/local_threads.py

.PHONY: test ## run all tests with all config types
test: clean_coverage isort lint flake8 mypy local_thread_test htex_local_test htex_local_alternate_test wqex_local_test vineex_local_test radical_local_test config_local_test perf_test ## run all tests

.PHONY: tag
tag: ## create a tag in git. to run, do a 'make VERSION="version string" tag
	./tag_and_release.sh create_tag $(VERSION)

.PHONY: package
package: ## package up a distribution.
	./tag_and_release.sh package

.PHONY: deploy
deploy: ## deploy the distribution
	./tag_and_release.sh release

# THIS IS MEANT TO BE INVOKED BY GITHUB ACTIONS **ONLY**
.PHONY: update_version
update_version: ## Update version
	./tag_and_release.sh update_version

.PHONY: release
release: deps tag package deploy   ## create a release. To run, do a 'make VERSION="version string"  release'

.PHONY: coverage
coverage: ## show the coverage report
	# coverage report
	echo no-op coverage report

.PHONY: clean
clean: ## clean up the environment by deleting the .venv, dist, eggs, mypy caches, coverage info, etc
	rm -rf .venv $(DEPS) dist *.egg-info .mypy_cache build .pytest_cache .coverage runinfo $(WORKQUEUE_INSTALL)
