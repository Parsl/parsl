Overview
========

Parsl is designed to enable the composition of asynchronous workflows in python. This is done in two steps:

1. Enable the markup of functions as parallel functions or ``App`` 's.
2. Specify the data dependencies between functions.

In Parsl the execution of an ``App`` yields `futures <https://en.wikipedia.org/wiki/Futures_and_promises>`_.
These futures can be passed to other ``App`` 's as inputs, establishing a data-dependency. This allows
you to create `directed acyclic graphs <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_
implictly. ``App`` 's which have all their dependencies resolved are slated for execution in parallel.
This allows Parsl to exploit all parallelism to fullest extent at the granularity expressed by the user.

A simple for Map Reduce job is as simple as this :

.. code-block:: python

    splits = os.
    for i in range(0,100):
        fu, _ = app_double(i)

    total = app_sum(inputs=fu)




