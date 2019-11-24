Overview
========

Parsl is designed to enable the straightforward orchestration of asynchronous tasks into dataflow-based workflows, in Python. Parsl manages the concurrent execution of these tasks across computation resources,
scheduling each task only when its dependencies (e.g., input data dependencies) are met.

Developing a workflow is a two-step process:

1. Define Parsl apps by annotating Python functions to indicate that they be executed concurrently.
2. Use standard Python code to create tasks and define dependencies among them, by calling such apps and defining data sharing.

The following example demonstrates how Parsl can be used to specify a MapReduce computation.

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
    items = range(0,4)

    # Map phase: apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = app_double(i)
        mapped_results.append(x)

    # Reduce phase: apply an *app* function to the set of results
    total = app_sum(inputs=mapped_results)

    print(total.result())

The program first defines two apps, `app_double` and `app_sum`,
It then makes four calls to the `app_double` app and one call to the `app_sum` app;
these execute concurrently, synchronized  by `mapped_result` variable.
The following figure shows the resulting task graph. 

.. image:: ../images/MapReduce.png

A call to a Parsl app results in the creation of one or more `futures <https://en.wikipedia.org/wiki/Futures_and_promises>`_,
representations of values that are to be computed in the future.
Futures enable synchronization among concurrently executing tasks,
as if they are passed as inputs to another app, that app can only execute when the app that is generating the future's value has completed execution.
Futures thus establish dependencies. 

In the example, each call to the `app_double` app returns a future, `x`; the resulting four futures are stored
in the list `mapped_results`; this list is provided as input to the `app_sum` app, which therefore cannot execute until all four values are available; and finally, the `app_sum` app computes the sum of the doubled values.

As the figure shows, a set of tasks and their dependencies form a  `directed acyclic graphs <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
Indeed, Parsl can be viewed as a notation and a set of mechanisms for defining and executing such graphs.
Note, however, that such graphs are never explicitly expressed in Parsl, and that  
the dependency graph is built dynamically and updated as the Parsl script executes. 
That is, the graph is not computed in advance and is only complete when the script finishes executing.
Apps that have all their dependencies met are slated for execution (in parallel).

