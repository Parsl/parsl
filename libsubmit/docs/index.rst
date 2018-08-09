.. libsubmit documentation master file, created by
   sphinx-quickstart on Mon Oct  2 13:39:42 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to libsubmit's documentation!
=====================================

Libsubmit is responsible for managing execution resources with a Local Resource
Manager (LRM). For instance, campus clusters and supercomputers generally have
schedulers such as Slurm, PBS, Condor and. Clouds on the other hand have API
interfaces that allow much more fine grain composition of an execution environment.
An execution provider abstracts these resources and provides a single uniform
interface to them.

This module provides the following functionality:

   1. A standard interface to schedulers
   2. Support for submitting, monitoring and cancelling jobs
   3. A modular design, making it simple to add support for new resources.
   4. Support for pushing files from client side to resources.


.. toctree::

   quick/quickstart
   userguide/index
   reference
   devguide/changelog
   devguide/dev_docs
   devguide/packaging


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
