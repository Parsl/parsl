template_string = """#!/bin/bash
#sed -i 's/us-east-2\.ec2\.//g' /etc/apt/sources.list
cd ~
apt-get update -y
apt-get install -y python3 python3-pip
pip3 install numpy scipy parsl

$user_script
"""
