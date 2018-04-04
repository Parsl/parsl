Parsl - Testing IPP
===================

These are notes on testing with Ipython Parallel workers connected to the Parsl DFK.
There are multiple modes of operation here :
1. The engines are started by ipcluster
2. The engines and the ipcontroller are started manually.



Ipcluster
---------

Start the cluster with ipcluster and 4 ipengines :

>>> ipcluster start -n 4


Manual
------

Manually start the ipcontroller and the engines:

First start the ipcontroller:

>>> ipcontroller --ip=<IP_address> --port=50001


Then start the engines, but before that make sure to copy the ~/.ipython/profile_default/security/ipcontroller-engine.json
file to the remote machine.

>>> ipengine --file=<path/to/ipcontroller-engine.json>

