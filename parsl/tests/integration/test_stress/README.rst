Profiling How-To
----------------

There's a heavy performance penalty when using conda vs standard python
distributions. So, these tests are preferably done with python installations
that come with the system.


To dump the profiling information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

> python3 -m cProfile -s cumtime -o <profiling.log> <test_1.py> -c 1000

To view the performance data :
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

> pyprof2calltree -k -i <profiling.log>
