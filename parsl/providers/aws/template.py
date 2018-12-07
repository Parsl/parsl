template_string = """#!/bin/bash
#sed -i 's/us-east-2\.ec2\.//g' /etc/apt/sources.list
cd ~
apt-get update -y
apt-get install -y python3 python3-pip libffi-dev g++ libssl-dev

# this will install parsl, from github
git clone https://github.com/parsl/parsl
cd parsl
git checkout benc-wip
pip3 install .

# previously, the command installed the latest release
# pip3 install parsl

# ... and during development, probably installing something closer to the
# local development branch is something desirable - eg by pushing to
# github and checking out that specific branch.

$worker_init

$user_script

# Shutdown the instance as soon as the worker scripts exits
# or times out to avoid EC2 costs.
if ! $linger
then
    halt
fi
"""
