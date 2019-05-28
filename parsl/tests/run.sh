#!/usr/bin/env bash


set -e
flake8 parsl/

# This uses all of the configurations and tests as the base from which to
# run mypy checks - these are likely to capture most of the code used in
# parsl
for test in parsl/tests/configs/*.py parsl/tests/test*/test*
  do
    MYPYPATH=$(pwd)/mypy-stubs mypy $test
    export MypyReturnCode=$?
    echo mypy return code is $MypyReturnCode
    if [[ "$MypyReturnCode" != 0 ]] ; then
      exit 1
    fi
  done

# do this before any testing, but not in-between tests
rm -f .coverage

for test in parsl/tests/test*/test*
  do pytest $test -k "not cleannet" --config parsl/tests/configs/htex_local.py --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    # allow exit code 5; this means pytest did not run a test in the
    # specified file
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

for test in parsl/tests/test*/test*
  do pytest $test -k "not cleannet" --config parsl/tests/configs/local_threads.py --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

# these tests run with specific configs loaded within the tests themselves.
# This mode is enabled with: --config local
for test in parsl/tests/test*/test*
  do pytest $test -k "not cleannet" --config local --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

# run simple worker test. this is unlikely to scale due to
# a stdout/stderr buffering bug in present master.
coverage run --append --source=parsl parsl/tests/manual_tests/test_worker_count.py -c 1000

# run specific integration tests that need their own configuration
pytest parsl/tests/integration/test_retries.py -k "not cleannet" --config local --cov=parsl --cov-append --cov-report=
pytest parsl/tests/integration/test_parsl_load_default_config.py -k "not cleannet" --config local --cov=parsl --cov-append --cov-report=

coverage report
# prints report of coverage data stored in .coverage
