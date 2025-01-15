#!/bin/bash -e

source /venv/bin/activate

pytest parsl/tests/ --config parsl/tests/ci_k8s/htex_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order

PYTHONPATH=/usr/lib/python3.11/site-packages/ pytest parsl/tests/ --config parsl/tests/ci_k8s/taskvine_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order --log-cli-level=DEBUG
