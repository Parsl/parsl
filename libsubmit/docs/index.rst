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

Contents:

.. toctree::
   :maxdepth: 3
   :caption: Quickstart

   quick/quickstart.rst


.. toctree::
   :maxdepth: 3
   :caption: User Guide

   userguide/overview

.. _dev_docs:
.. toctree::
   :maxdepth: 4
   :caption: Developer Documentation

   devguide/design
   devguide/dev_docs
   devguide/packaging

.. toctree::
   :maxdepth: 4
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
