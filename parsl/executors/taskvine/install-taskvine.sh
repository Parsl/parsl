#!/bin/bash

set -xe

if [[ -z $CCTOOLS_VERSION ]]; then
  echo Environment variable CCTOOLS_VERSION must be set
  exit 1
fi

TARBALL="cctools-$CCTOOLS_VERSION-x86_64-ubuntu20.04.tar.gz"
[[ -f "/etc/redhat-release" ]] && TARBALL="cctools-$CCTOOLS_VERSION-x86_64-centos8.tar.gz"

# If stderr is *not* a TTY, then disable progress bar and show HTTP response headers
[[ ! -t 1 ]] && NO_VERBOSE="--no-verbose" SHOW_HEADERS="-S"
wget "$NO_VERBOSE" "$SHOW_HEADERS" -O /tmp/cctools.tar.gz "https://github.com/cooperative-computing-lab/cctools/releases/download/release/$CCTOOLS_VERSION/$TARBALL"

mkdir -p /tmp/cctools
tar -C /tmp/cctools -zxf /tmp/cctools.tar.gz --strip-components=1

# install taskvine additional dependencies that are not included in the binary tarball
pip install cloudpickle
