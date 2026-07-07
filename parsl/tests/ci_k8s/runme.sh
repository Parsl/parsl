#!/bin/bash -ex

source /venv/bin/activate

pytest parsl/tests/ --config parsl/tests/ci_k8s/htex_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order

PYTHONPATH=/usr/lib/python3.11/site-packages/ pytest parsl/tests/ --config parsl/tests/ci_k8s/taskvine_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order --log-cli-level=DEBUG || true

# || true to keep running so I can add in debug hooks here
# TODO: remove/fixup before merge


echo find /tmp/
ls -l /tmp
ls -l /tmp/
find /tmp/

echo pwd
pwd
echo find .
find .

echo ps ax
ps ax

pushd ./pytest-parsl/*taskvine*

for f in $(find . -type f); do
  echo FILE $f
  cat $f
  echo END FILE $f
done

echo end
