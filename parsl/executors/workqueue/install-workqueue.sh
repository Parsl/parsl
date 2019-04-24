#!/bin/bash
wget -O /tmp/workqueue_latest https://api.github.com/repos/cooperative-computing-lab/cctools/releases/latest
url=$(cat /tmp/cctools_latest | grep "ubuntu" | grep "browser_download_url" | sed -E "s/^ +\"browser_download_url\": \"(.*)\"/\1/g")
wget -o /tmp/cctools.tar.gz "$url"
tar zxvf cctools.tar.gz
export PATH=/tmp/cctools/bin:$PATH
export PYTHONPATH=/tmp/cctools/lib/python3.6/site-packages:$PYTHONPATH