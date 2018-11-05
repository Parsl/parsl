Simple Dealer-Reply Workflow using ZeroMQ
=========================================

The ``simple_dealer_rep.py`` module contains a simple proof of concept of a 
Dealer-Reply combination for fast job scheduling. Running the script from the 
command line runs a dummy job with a simple ``double`` function any given 
number of times. The dummy job is run both with the Dealer-Rep combination and 
as a serial job (without ZeroMQ). The script prints average times taken for 
serialization and execution in both cases. An example is shown below.

.. code:: bash

    $ python3 simple_dealer_rep.py --num-tasks 100000 --num-workers 1
        [SIMPLE-DEALER-REP] Avg serialization time:  77.5579 us
        [SIMPLE-DEALER-REP] Avg execution time:      53.8547 us
        [WITHOUT-ZEROMQ] Avg serialization time:     50.5816 us
        [WITHOUT-ZEROMQ] Avg execution time:         64.8208 us
