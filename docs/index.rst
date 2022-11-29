Parsl - Parallel Scripting Library
##################################

Parsl is parallel programming library for Python for parallelism beyond a single computer.

At the easiest level, you can use Parsl to run functions just like Python's executors but across *multiple computers*:

.. code-block:: python

    from parsl.concurrent import ParslPoolExecutor
    from parsl.configs.htex_local import config

    with ParslPoolExecutor(config) as exec:
        results = pool.map(function, [...])

Start with the `configuration quickstart <quickstart.html#getting-started>`_ to learn how to tell Parsl how to use your computing resource.
Parsl can run on most supercomputers and with major cloud providers.

The real power of Parsl comes is expressing multi-step workflows of functions.
Parsl lets you chain functions together and will launch each function as soon as possible.

.. code-block:: python

    import parsl
    from parsl import python_app

    # Start Parsl on a single computer
    parsl.load()

    # Make functions parallel by decorating them
    @python_app
    def f(x):
        return x + 1

    @python_app
    def g(x):
        return x * 2

    # These functions now return Futures, and can be chained
    future = f(1)
    assert future.result() == 2

    future = g(f(1))
    assert future.result() == 4


Parsl is an open-source code, and available on GitHub: https://github.com/parsl/parsl/

Table of Contents
+++++++++++++++++

.. toctree::
   :maxdepth: 2

   quickstart
   1-parsl-introduction.ipynb
   userguide/index
   faq
   reference
   devguide/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
