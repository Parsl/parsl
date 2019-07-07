template_string = """#!/bin/bash
cd ~
apt-get update -y
apt-get install -y python3 python3-pip libffi-dev g++ libssl-dev
pip3 install numpy scipy parsl
$worker_init
$user_script
# Shutdown the instance as soon as the worker scripts exits
# or times out to avoid Azure costs.
if ! $linger
then
    halt
fi
"""
