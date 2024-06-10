#!/bin/bash

source /venv/bin/activate

pytest parsl/tests/ --config ./htex_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order


