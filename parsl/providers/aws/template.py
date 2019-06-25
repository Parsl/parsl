template_string = """#!/bin/bash
#sed -i 's/us-east-2\.ec2\.//g' /etc/apt/sources.list
cd ~
apt-get update -y
apt-get install -y python3 python3-pip libffi-dev g++ libssl-dev
pip3 install numpy scipy
pip3 install git+https://github.com/macintoshpie/parsl.git@debug/aws
$worker_init

$user_script

# Shutdown the instance as soon as the worker scripts exits
# or times out to avoid EC2 costs.
if ! $linger
then
    halt
fi
"""
