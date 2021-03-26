#!/bin/bash -ex

# pass any argument to make this script generate a fresh monitoring database
# using pytest; otherwise, by default, it will test against an existing
# monitoring.db

killall --wait parsl-visualize || echo No previous parsl-visualize to kill

if  [ -n "$1" ]; then
  rm -f monitoring.db
  pytest parsl/tests/ -k "not cleannet" --config parsl/tests/configs/htex_local_alternate.py --cov=parsl --cov-append --cov-report= --random-order
fi

parsl-visualize &

mkdir -p test-parsl-visualize.tmp
cd test-parsl-visualize.tmp

# now wait for this to become responsive to connections
wget http://127.0.0.1:8080/ --retry-connrefused --tries 30 --waitretry=1
# wget will return a failure code if any of the requested URLs don't return an HTTP 200 result
wget http://127.0.0.1:8080/ --recursive --no-verbose --page-requisites --level=inf -e robots=off
killall --wait parsl-visualize

