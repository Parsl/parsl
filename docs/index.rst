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

.. note::
   Parsl collects anonymous usage statistics for reporting and
   improvement purposes. To understand what stats are collected and to disable
   collection please refer to the `usage tracking guide <http://parsl.readthedocs.io/en/latest/userguide/usage_tracking.html>`__


.. toctree::

   quickstart
   parsl-introduction.ipynb
   userguide/index
   faq
   reference
   devguide/index

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
