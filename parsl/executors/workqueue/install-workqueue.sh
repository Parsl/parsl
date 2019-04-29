#!/bin/bash
wget -O /tmp/cctools_latest https://api.github.com/repos/cooperative-computing-lab/cctools/releases/latest
url=$(cat /tmp/cctools_latest | grep "ubuntu" | grep "browser_download_url" | sed -E "s/^ +\"browser_download_url\": \"(.*)\"/\1/g")
wget -O /tmp/cctools.tar.gz "$url"
mkdir /tmp/cctools
tar -C /tmp/cctools -zxvf /tmp/cctools.tar.gz --strip-components=1