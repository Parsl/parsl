template_string = """#!/bin/bash
cd ~
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y python3 python3-pip libffi-dev g++ libssl-dev
pip3 install numpy scipy parsl

if $upload_parsl
then
    # Wait until the Parsl SCP upload is complete
    while ! (test -f "/tmp/parsl_upload_complete"); do
        sleep 5
    done
    python3 /tmp/parsl/providers/aws/install_uploaded_parsl.py
fi

$worker_init

$user_script

# Shutdown the instance as soon as the worker scripts exits
# or times out to avoid EC2 costs.
if ! $linger
then
    halt
fi
"""
