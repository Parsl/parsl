set -e

flake8 parsl/
bash parsl/tests/lint-inits.sh

# This uses all of the configurations and tests as the base from which to
# run mypy checks - these are likely to capture most of the code used in
# parsl
MYPYPATH=$(pwd)/mypy-stubs mypy parsl/tests/configs/
MYPYPATH=$(pwd)/mypy-stubs mypy parsl/tests/test*/
MYPYPATH=$(pwd)/mypy-stubs mypy parsl/tests/sites/

# do this before any testing, but not in-between tests
rm -f .coverage

pytest parsl -k "not cleannet" --config parsl/tests/configs/htex_local.py --cov=parsl --cov-append --cov-report= --random-order
pytest parsl -k "not cleannet" --config parsl/tests/configs/local_threads.py --cov=parsl --cov-append --cov-report= --random-order

# some of the site/ tests require more dependencies. These are installed here as needed,
# so that the above tests happen with only the basic requirements installed.

# workqueue
./parsl/executors/workqueue/install-workqueue.sh
export PATH=$PATH:/tmp/cctools/bin
export PYTHONPATH=/tmp/cctools/lib/python3.5/site-packages

# mpi
bash parsl/executors/extreme_scale/install-mpi.sh $MPI
if [[ "$MPI" == "mpich"   ]]; then mpichversion; fi
if [[ "$MPI" == "openmpi" ]]; then ompi_info;    fi

pip install .[extreme_scale,monitoring]

work_queue_worker localhost 9000 &> /dev/null &

pytest parsl -k "not cleannet" --config parsl/tests/configs/workqueue_ex.py --cov=parsl --cov-append --cov-report= --random-order --bodge-dfk-per-test
kill -3 $(ps aux | grep -E -e "[0-9]+:[0-9]+ work_queue_worker" | tr -s ' ' | cut -f 2 -d " ")

# these tests run with specific configs loaded within the tests themselves.
# This mode is enabled with: --config local
pytest parsl -k "not cleannet" --config local --cov=parsl --cov-append --cov-report= --random-order

# run simple worker test. this is unlikely to scale due to
# a stdout/stderr buffering bug in present master.
# - coverage run --append --source=parsl parsl/tests/manual_tests/test_worker_count.py -c 1000
# TODO: ^ this test has been removed pending investigation ... when restored or removed,
#       sort out this commented out block appropriately.

# prints report of coverage data stored in .coverage
coverage report
