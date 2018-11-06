Simple Dealer-Reply Workflow using ZeroMQ
=========================================

The ``simple_dealer_rep.py`` module contains a simple proof of concept of a 
Dealer-Reply combination for fast job scheduling. Running the script from the 
command line runs a dummy job with a simple ``double`` function any given 
number of times. The dummy job is run with the Dealer-Rep combination,
with the Dealer-Interchange-Rep combination, and as a serial job (without 
ZeroMQ). The script prints average times taken for serialization and execution 
in all cases. An example is shown below.

.. code:: bash

    $ python3 simple_dealer_rep.py --num-tasks 100000 --num-workers 1
        [DEALER-REP] Avg serialization time:              85.3889 us
        [DEALER-REP] Avg execution time:                  57.8621 us
        [DEALER-INTERCHANGE-REP] Avg serialization time:  97.0031 us
        [DEALER-INTERCHANGE-REP] Avg execution time:      75.0557 us
        [WITHOUT-ZEROMQ] Avg serialization time:          52.1529 us
        [WITHOUT-ZEROMQ] Avg execution time:              69.0042 us
