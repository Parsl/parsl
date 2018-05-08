.. Parsl documentation master file, created by
   sphinx-quickstart on Mon Feb 20 16:35:17 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Parsl - Parallel Scripting Library
##################################

Parsl is a Python-based parallel scripting library that supports development and execution of asynchronous and parallel data-oriented workflows (dataflows). These workflows glue together existing executables (called Apps) and Python functions with control logic written in Python. Parsl brings implicit parallel execution to standard Python scripts. Rather than explicitly defining a graph and/or modifying data structures, instead developers simply annotate Python functions and Apps. Parsl constructs a dynamic, parallel execution graph derived from the implicit linkage between Apps based on shared input/output data objects. Parsl then executes these components when dependencies are met. Parsl is resource-independent, that is, the same Parsl script can be executed on your laptop through to clusters, clouds, and supercomputers. Parsl also supports different executors including local threads, pilot jobs, and extreme-scale execution with Swift/T.

Parsl can be used to realize a variety of workflows:

* Parallel task-based workflows in which tasks are executed when their dependencies are met.
* Interactive and dynamic workflows in which the workflow is dynamically expanded during execution by users or the workflow itself.
* Procedural workflows in which serial execution of tasks are managed by Parsl.
* Workflows with many short duration tasks where no task-level fault tolerance is required
* Workflows with long running tasks with fault tolerance

.. note::
   By default, Parsl collects anonymous usage statistics for reporting and
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
