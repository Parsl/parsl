Running Workflows
=================

Parsl workflows are a Python "main" program that defines Apps,
how the Apps are invoked,
and how results are passed between different Apps.

The core concept of workflows is that Parsl Apps produce **Futures**
with all features from those in Python's :mod:`concurrent.futures` module and more.

.. toctree::
   :maxdepth: 2

   futures
   workflow
   exceptions
   lifted_ops
   checkpoints
