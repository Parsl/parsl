Overview
========

Parsl is designed to enable the straightforward orchestration of asynchronous tasks into dataflow-based workflows in Python. Parsl manages the parallel execution of these tasks across computation resources when dependencies (e.g., input data dependencies) are met.

Developing a workflow is a two-step process: 

1. Annotate functions that can be executed in parallel as Parsl ``Apps``.
2. Specify dependencies between functions using standard Python code.

In Parsl, the execution of an ``App`` yields `futures <https://en.wikipedia.org/wiki/Futures_and_promises>`_.
These futures can be passed to other ``Apps`` as inputs, establishing a dependency. These dependencies are assembled  implicitly into `directed acyclic graphs <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_,
although these are never explicitly expressed. It is important to note that this graph is dynamically built and then update while the Parsl script executes. That is, the graph is not computed in advanced and is only complete when the script finishes executing. 
``Apps`` that have all their dependencies met are slated for execution (in parallel).
This allows Parsl to exploit all parallelism to the fullest extent and at the granularity expressed by the user.

At the heart of Parsl is the DataFlow Kernel (DFK). The DFK is responsible for managing the dynamic graph and determining when tasks can be executed. 

A MapReduce job can be simply defined as follows:

.. code-block:: python

    # Map function that returns double the input integer
    @App('python', dfk)
    def app_double(x):
        return x*2

    # Reduce function that returns the sum of a list
    @App('python', dfk)
    def app_sum(inputs=[]):
        return sum(inputs)

    # Create a list of integers
    items = range(0,N)

    # Map phase: apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = app_double(i)
        mapped_results.append(x)

    # Reduce phase: apply an *app* function to the set of results
    total = app_sum(inputs=mapped_results)

    print(total.result())
