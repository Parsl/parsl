export PARSL_TESTING="true"
#pip install -r test-requirements.txt

set -e
flake8 parsl/

parsl/tests/lint-inits.sh
set +e

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
  do
    pytest $test -k "not cleannet" --config parsl/tests/configs/htex_local.py --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    # allow exit code 5; this means pytest did not run a test in the specified file
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

for test in parsl/tests/test*/test*
  do pytest $test -k "not cleannet" --config parsl/tests/configs/local_threads.py --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    # allow exit code 5; this means pytest did not run a test in the specified file
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

# some of the site/ tests require more dependencies. These are installed here as needed,
# so that the above tests happen with only the basic requirements installed.

set -e
# workqueue
bash parsl/executors/workqueue/install-workqueue.sh
export PATH=$PATH:/tmp/cctools/bin
export PYTHONPATH=/tmp/cctools/lib/python3.5/site-packages

# mpi
bash parsl/executors/extreme_scale/install-mpi.sh $MPI
if [[ "$MPI" == "mpich"   ]]; then mpichversion; fi
if [[ "$MPI" == "openmpi" ]]; then ompi_info;    fi

pip install .[extreme_scale,monitoring]

work_queue_worker localhost 9000 &> /dev/null &

set +e
for test in parsl/tests/test*/test*
  do
    pytest $test -k "not cleannet" --config parsl/tests/configs/workqueue_ex.py --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

kill -3 $(ps aux | grep -E -e "[0-9]+:[0-9]+ work_queue_worker" | tr -s ' ' | cut -f 2 -d " ")

# These tests run with specific configs loaded within the tests themselves.
# This mode is enabled with: --config local
for test in parsl/tests/test*/test* parsl/tests/sites/test*
  do
    pytest $test -k "not cleannet" --config local --cov=parsl --cov-append --cov-report=
    export PytestReturnCode=$?
    echo pytest return code is $PytestReturnCode
    if [[ "$PytestReturnCode" != 0 ]] && [[ "$PytestReturnCode" != 5 ]]; then
      exit 1
    fi
  done

set -e
# run simple worker test. this is unlikely to scale due to
# a stdout/stderr buffering bug in present master.
coverage run --append --source=parsl parsl/tests/manual_tests/test_worker_count.py -c 1000

# run specific integration tests that need their own configuration
pytest parsl/tests/integration/test_retries.py -k "not cleannet" --config local --cov=parsl --cov-append --cov-report=
pytest parsl/tests/integration/test_parsl_load_default_config.py -k "not cleannet" --config local --cov=parsl --cov-append --cov-report=

coverage report
# prints report of coverage data stored in .coverage

pytest parsl/tests --config parsl/tests/configs/local_threads.py
pytest parsl/tests --config parsl/tests/configs/local_ipp.py
