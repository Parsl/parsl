template_string = """#!/bin/bash
cd ~

# Power off (which terminates the instance, as it is launched with
# InstanceInitiatedShutdownBehavior=terminate) when this script exits for
# any reason, including failures below, and in any case once the walltime
# has elapsed, to avoid EC2 costs.
if ! $linger
then
    trap 'shutdown -h now' EXIT
    shutdown -h +$walltime_minutes
fi

# Install Python, preferring the same python3.X as the submitting side:
# the HighThroughputExecutor interchange rejects workers running a
# different Python minor version. Fall back to the default python3.
if command -v apt-get > /dev/null
then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y python3 python3-pip python3-venv libffi-dev g++ libssl-dev
    apt-get install -y python$python_version python$python_version-venv
elif command -v dnf > /dev/null
then
    dnf install -y python$python_version python$python_version-pip || dnf install -y python3 python3-pip
fi

if command -v python$python_version > /dev/null
then
    PARSL_PYTHON=python$python_version
else
    PARSL_PYTHON=python3
fi

# Install Parsl into a virtual environment: recent Debian and Ubuntu
# releases refuse system-wide pip installs (PEP 668), and the worker must
# run the same Parsl version as the submitting side.
$$PARSL_PYTHON -m venv /opt/parsl-venv
/opt/parsl-venv/bin/python -m pip install $parsl_requirement
export PATH="/opt/parsl-venv/bin:$$PATH"

$worker_init

# Run the worker command in a subshell, so that options such as set -e
# which it sets (as the default SingleNodeLauncher does) do not leak into
# this script.
(
$user_script
)
"""
