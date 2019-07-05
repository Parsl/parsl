.. Parsl documentation master file, created by
   sphinx-quickstart on Mon Feb 20 16:35:17 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Parsl - Parallel Scripting Library
##################################

Parsl is a Python library for programming and executing data-oriented workflows (dataflows) in parallel.
Parsl  scripts  allow  selected Python  functions  and  external  applications  (called apps)  to
be connected by shared input/output data objects into flexible parallel workflows. 
Rather than explicitly defining a dependency graph and/or modifying data structures, instead 
developers simply annotate Python functions and Parsl constructs a dynamic, parallel execution 
graph derived from the implicit linkage between apps based on shared input/output data objects. 
Parsl then executes apps when dependencies are met. Parsl is resource-independent, 
that is, the same Parsl script can be executed on a laptop, cluster, cloud, or supercomputer. 

Parsl can be used to realize a variety of workflows:

* Parallel dataflow in which tasks are executed when their dependencies are met.
* Interactive and dynamic workflows in which the workflow is dynamically expanded during execution by users or the workflow itself.
* Procedural workflows in which serial execution of tasks are managed by Parsl.

=======
.. libsubmit documentation master file, created by
.. sphinx-quickstart on Mon Oct  2 13:39:42 2017
.. You can adapt this file completely to your liking, but it should at least
.. contain the root `toctree` directive.


.. Libsubmit is responsible for managing execution resources with a Local Resource
.. Manager (LRM). For instance, campus clusters and supercomputers generally have
.. schedulers such as Slurm, PBS, Condor and. Clouds on the other hand have API
.. interfaces that allow much more fine grain composition of an execution environment.
.. An execution provider abstracts these resources and provides a single uniform
.. interface to them.

.. This module provides the following functionality:

..    1. A standard interface to schedulers
..    2. Support for submitting, monitoring and cancelling jobs
..    3. A modular design, making it simple to add support for new resources.
..    4. Support for pushing files from client side to resources.


.. toctree::

   quickstart
   parsl-introduction.ipynb
   userguide/index
   faq
   reference
   devguide/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
