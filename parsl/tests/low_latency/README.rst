Simple Dealer-Reply Workflow using ZeroMQ
=========================================

The modules in this folder contain a simple proof of concept of a 
Dealer-Reply combination for fast job scheduling. Running the ``benchmark.py``
script from the command line runs a dummy job with a simple ``double`` 
function any given number of times. The dummy job is run with the Dealer-Rep 
combination, with the Dealer-Interchange-Rep combination, and as a serial job 
(without ZeroMQ). The script prints average times taken for serialization and execution in all cases. An example is shown below.

.. code:: bash

    $ python3 simple_dealer_rep.py
        [WITHOUT-ZEROMQ] Avg Serialization Time
        Mean =    81.4691 us, Stdev =   107.1882 us
        [WITHOUT-ZEROMQ] Avg Execution Time
        Mean =    65.1698 us, Stdev =     0.5487 us
        [DEALER-REP] Avg Serialization Time
        Mean =    52.6101 us, Stdev =     0.1870 us
        [DEALER-REP] Avg Execution Time
        Mean =   471.9303 us, Stdev =     3.5830 us
        [DEALER-INTERCHANGE-REP] Avg Serialization Time
        Mean =    55.1373 us, Stdev =     0.6979 us
        [DEALER-INTERCHANGE-REP] Avg Execution Time
        Mean = 51290.3344 us, Stdev =   442.0344 us
