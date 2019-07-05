Overview
========

Parsl is designed to enable the straightforward orchestration of asynchronous tasks into dataflow-based workflows in Python. Parsl manages the parallel execution of these tasks across computation resources when dependencies (e.g., input data dependencies) are met.

Developing a workflow is a two-step process:

1. Annotate functions that can be executed in parallel as Parsl apps.
2. Specify dependencies between functions using standard Python code.

In Parsl, the execution of an app yields `futures <https://en.wikipedia.org/wiki/Futures_and_promises>`_.
These futures can be passed to other apps as inputs, establishing a dependency. These dependencies are assembled implicitly into `directed acyclic graphs <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_,
although these are never explicitly expressed. Furthermore, the dependency graph is dynamically built and then updated while the Parsl script executes. That is, the graph is not computed in advance and is only complete when the script finishes executing.
Apps that have all their dependencies met are slated for execution (in parallel).


The following example demonstrates how a MapReduce job can be defined.

.. code-block:: python

    from parsl import load, python_app
    from parsl.configs.local_threads import config
    load(config)

    # Map function that returns double the input integer
    @python_app
    def app_double(x):
        return x*2

    # Reduce function that returns the sum of a list
    @python_app
    def app_sum(inputs=[]):
        return sum(inputs)

    # Create a list of integers
    items = range(0,10)

    # Map phase: apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = app_double(i)
        mapped_results.append(x)

    # Reduce phase: apply an *app* function to the set of results
    total = app_sum(inputs=mapped_results)

    print(total.result())
