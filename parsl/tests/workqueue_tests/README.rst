Testing WQ+Parsl
================

Setting up:
1. Make sure to install wq with the same python3 version that you use for testing.


To run the scale test::
  $ python3 test_scale.py -d -c 10 -f <config_file_without_.py_extension>

Eg::
  $ python3 test_scale.py -d -c 10 -f htex_local
  $ python3 test_scale.py -d -c 10 -f local_threads
  $ python3 test_scale.py -d -c 10 -f wqex_local
