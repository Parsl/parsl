Running Workflows
=================

Parsl workflows are a Python "main" program that defines Apps,
how the Apps are invoked,
and how results are passed between different Apps.

The core concept of workflows is that Parsl Apps produce **Futures**
with all features from those in Python's :mod:`concurrent.futures` module and more.

The Parsl programs typically contain a ``parsl.load`` statement to start
the services which manage scheduling computations.

.. code-block:: python

    from parsl.configs.htex_local import config  # See the "Configuration" documentation
    from parsl import python_app  # See the "Apps" documentation
    import parsl

    @python_app
    def f(x):
        return x + 1

    with parsl.load(config):
        future = f(1)
        print(f'1 + 1 = {future.result()}')


.. toctree::
   :maxdepth: 2

   futures
   workflow
   exceptions
   lifted_ops
   logging
   checkpoints
   concurrent
