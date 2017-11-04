Overview
========

Parsl is designed to enable the composition of asynchronous tasks into workflows in python.
Parsl workflows are portable across a variety of computation platforms and exploit many-task parallelism.
A workflow is composed in two steps:

1. The markup of functions as parallel functions or ``Apps``.
2. Specification of data dependencies between functions.

In Parsl, the execution of an ``App`` yields `futures <https://en.wikipedia.org/wiki/Futures_and_promises>`_.
These futures can be passed to other ``Apps`` as inputs, establishing a data-dependency. This allows
you to create implicit `directed acyclic graphs <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_,
though these are never explicitly expressed, either by the programmer or internally in Parsl.
``Apps`` that have all their dependencies resolved are slated for execution in parallel.
This allows Parsl to exploit all parallelism to fullest extent at the granularity expressed by the user.

A MapReduce job can be as simple as this:

.. code-block:: python

    # Map Function that returns doubles the input integer
    @App('python', dfk)
    def app_double(x):
        return x*2

    # Reduce function that returns the sum of a list
    @App('python', dfk)
    def app_sum(inputs=[]):
        return sum(inputs)

    # Create a list of integers
    items = range(0,N)

    # Map Phase : Apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = app_double(i)
        mapped_results.append(x)

    total = app_sum(inputs=mapped_results)

    print(total.result())
