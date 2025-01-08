.. _apps:

Writing Parsl Apps
==================

An **App** defines a computation that will be executed asynchronously by Parsl.
Apps are Python functions marked with a decorator which
designates that the function will run asynchronously and cause it to return
a :class:`~concurrent.futures.Future` instead of the result.

Apps can be one of three types of functions, each with their own type of decorator

- ``@python_app``: Most Python functions
- ``@bash_app``: A Python function which returns a command line program to execute
- ``@join_app``: A function which launches one or more new Apps

Start by learning how to write Python Apps, which define most of the rules needed to write
other types of Apps.

.. toctree::
   :maxdepth: 1

   python
   bash
   mpi_apps
   joins
