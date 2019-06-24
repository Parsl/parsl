#!/bin/bash -e

# Check that each directory with python source in it has a __init__.py
# file in it too. Often code will work in most situations without an
# __init__.py but some corner cases can arise that are awkward to debug;
# so the policy is that every directory must have __init__.py

find parsl -name \*.py \
  | while read fn; do
      dirname $fn
    done \
  | sort | uniq \
  | while read dir; do
      if [ -f ${dir}/__init__.py ] ;
        then true;
        else echo FAILURE: $dir does not have an __init__.py ; exit 1;
      fi; 
    done
