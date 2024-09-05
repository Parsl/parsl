#!/bin/bash -e

source /venv/bin/activate

pytest parsl/tests/ --config ./htex_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order


# I tried letting staging_required tests run here but they do not -- a bit confused about this comment in taskvine:
#
#            # Absolute paths are assumed to be in shared filesystem, and thus
#            # not staged by taskvine.
# which I guess is saying something is making assumptions about the presence of a shared filesystem even when defaulting to shared_fs=False in the taskvine config?


PYTHONPATH=/usr/lib/python3.12/site-packages/ pytest parsl/tests/ --config ./taskvine_k8s_kind.py -k 'not issue3328 and not staging_required and not shared_fs' -x --random-order --log-cli-level=DEBUG
