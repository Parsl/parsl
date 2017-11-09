.. Parsl documentation master file, created by
   sphinx-quickstart on Mon Feb 20 16:35:17 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Parsl - Parallel Scripting Library
##################################

Parsl is a Python-based parallel scripting library that supports development and execution of asynchronous and parallel data-oriented workflows (dataflows) that glue together existing executables (called Apps) and functions. Building upon the Swift parallel scripting language, Parsl brings implicit parallel execution support to standard Python scripts. Rather than explicitly defining a graph and/or modifying data structures, instead developers simply annotate selected existing Python functions and Apps. Parsl constructs a dynamic, parallel execution graph derived from the implicit linkage between Apps based on shared input/output data objects. Parsl is resource-independent, that is, the same Parsl script can be executed on arbitrary computational resources from multicore processors through to clusters, clouds, and supercomputers.

Contents:

.. toctree::
   :maxdepth: 3
   :caption: Quickstart

   quick/quickstart.rst
   quick/Tutorial.rst

.. toctree::
   :maxdepth: 3
   :caption: User Guide

   userguide/overview
   userguide/apps
   userguide/futures

.. _dev_docs:
.. toctree::
   :maxdepth: 4
   :caption: Developer Documentation

   devguide/design
   devguide/roadmap
   devguide/dev_docs
   devguide/packaging


Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

