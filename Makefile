PYTHON := $(shell which python3 || echo ".python_is_missing")
SHELL := $(shell which bash) # Use bash instead of bin/sh as shell
GIT := $(shell which git || echo ".git_is_missing")
CWD := $(shell pwd)
CCTOOLS_INSTALL := /tmp/cctools
MPICH=mpich
OPENMPI=openmpi
export PATH := $(CCTOOLS_INSTALL)/bin:$(PATH)
export CCTOOLS_VERSION=7.7.2
export HYDRA_LAUNCHER=fork
export OMPI_MCA_rmaps_base_oversubscribe=yes
MPI=$(MPICH)

TOX_TARGETS= mypy local_thread_test htex_local_test htex_local_alternate_test radical_local_test config_local_test site_test_selector site_test_local perf_test

.PHONY: help
help: ## me
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@/bin/echo -e "Additionally, the main test targets are:\n  $(TOX_TARGETS)"

VENV = .venv
.PHONY: virtualenv
virtualenv: ## create an activate a virtual env
	test -f $(VENV)/bin/activate || $(PYTHON) -m venv $(VENV)
	echo "Run 'source $(VENV)/bin/activate' to activate the virtual environment"

.PHONY: lint
lint: ## run linter script
	parsl/tests/lint-inits.sh

.PHONY: flake8
flake8:  ## run flake
	flake8 parsl/

.PHONY: $(TOX_TARGETS)
$(TOX_TARGETS):  ## Run set of tests
	tox -e $@

$(CCTOOLS_INSTALL):	#CCtools contains both taskvine and workqueue so install only once
	parsl/executors/taskvine/install-taskvine.sh

.PHONY: wqex_local_test vineex_local_test
wqex_local_test vineex_local_test: $(CCTOOLS_INSTALL)
	tox -e $@

.PHONY: site_test
site_test: site_test_selector site_test_local

.PHONY: test ## run all tests with all config types
test: lint flake8 mypy local_thread_test htex_local_test htex_local_alternate_test wqex_local_test vineex_local_test radical_local_test config_local_test perf_test ## run all tests

.PHONY: tag
tag: ## create a tag in git. to run, do a 'make VERSION="version string" tag
	./tag_and_release.sh create_tag $(VERSION)

# THIS IS MEANT TO BE INVOKED BY GITHUB ACTIONS **ONLY**
.PHONY: update_version
update_version: ## Update version
	./tag_and_release.sh update_version

.PHONY: package
package: ## package up a distribution.
	@tox -e build

.PHONY: deploy
deploy: ## deploy the distribution
	@echo "======================================================================="
	@echo "Push to PyPi. This will require your username and password"
	@echo "======================================================================="
	@tox -e deploy

.PHONY: release
release: tag package deploy   ## create a release. To run, do a 'make VERSION="version string"  release'

.PHONY: clean
clean: ## clean up the environment by deleting the .venv, dist, eggs, mypy caches, coverage info, etc
	rm -rf .venv dist *.egg-info .mypy_cache build .pytest_cache .coverage runinfo_* $(WORKQUEUE_INSTALL)
