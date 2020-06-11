.. Parsl documentation master file, created by
   sphinx-quickstart on Mon Feb 20 16:35:17 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Parsl - Parallel Scripting Library
##################################

Parsl is a flexible and scalable parallel programming library for Python. 
Parsl augments Python with simple constructs for encoding parallelism. Developers annotate
Python functions to specify opportunities for concurrent execution. These annotated
functions, called ``apps``, may represent pure Python functions or calls to external
applications. Parsl further allows invocations of these apps, called ``tasks``, to be 
connected by shared input/output data (e.g., Python objects or files) via which Parsl 
constructs a dynamic dependency graph of tasks to manage concurrent task execution where possible.

Parsl includes an extensible and scalable runtime that allows it to efficiently execute
Parsl programs on one or many processors. Parsl programs are portable, enabling them
to be easily moved between different execution resources: from laptops to supercomputers. 
When executing a Parsl program, developers must define (or import) a Python 
configuration object that outlines where and how to execute tasks. Parsl supports
various target resources including clouds (e.g., Amazon Web Services and Google
Cloud), clusters (e.g., using Slurm, Torque/PBS, HTCondor, Cobalt), and container
orchestration systems (e.g., Kubernetes). Parsl scripts can scale from several
cores on a single computer through to hundreds of thousands of cores across many
thousands of nodes on a supercomputer. 

Parsl can be used to implement various parallel computing paradigms:

* Concurrent execution of tasks in a bag-of-tasks program.
* Procedural workflows in which tasks are executed following control logic.
* Parallel dataflow in which tasks are executed when their data dependencies are met.
* Many-task applications in which many computing resources are used to perform various computational tasks.
* Dynamic workflows in which the workflow is dynamically determined during execution.
* Interactive parallel programming through notebooks or interactive.

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
