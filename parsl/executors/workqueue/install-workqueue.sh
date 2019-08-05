#!/bin/bash
wget -O /tmp/cctools.tar.gz https://github.com/cooperative-computing-lab/cctools/releases/download/release/$CCTOOLS_VERSION/cctools-$CCTOOLS_VERSION-x86_64-ubuntu16.04.tar.gz
mkdir /tmp/cctools
tar -C /tmp/cctools -zxvf /tmp/cctools.tar.gz --strip-components=1