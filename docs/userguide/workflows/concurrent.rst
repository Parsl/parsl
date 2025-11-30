.. _label-parslpool:

ParslPoolExecutor Interface
===========================

The :class:`parsl.concurrent.ParslPoolExecutor` implements the "Executor" interface
from :mod:`concurrent.futures` module in standard Python.
This reduced interface permits employing Parsl into applications already capable
of single-node parallelism using the :class:`~concurrent.futures.ProcessPoolExecutor`
available in most versions of Python.

Creating a ParslPoolExecutor
----------------------------

.. note::

    See the :ref:`Configuration section <configuration-section>` for how to define computational resources.

Create a :class:`~parsl.concurrent.ParslPoolExecutor` using one of two methods:

1. Supplying a Parsl :class:`~parsl.Config` that defines how to create new workers.
   The executor will start a new Parsl Data Flow Kernel (DFK) when it is entered as a context manager.

   .. code-block:: python

       from parsl.concurrent import ParslPoolExecutor
       from parsl.config.htex_local import config  # Mimicks the PythonPool

       with ParslPoolExecutor(config=config) as pool:
           ...

   All resources will be closed when the block exits.


2. Supplying an already-started Parsl :class:`~parsl.DataFlowKernel` (DFK).
   The Parsl DFK must be started and stopped separate from the executor.

    .. code-block:: python

       from parsl.concurrent import ParslPoolExecutor
       from parsl.config.htex_local import config
       import parsl

       with parsl.load(dfk) as dfk
           with ParslPoolExecutor(dfk=dfk) as pool:
               ...
           ...

   Parsl will only shut when the outer block exits.

Use multiple types of resources within the same program
by creating multiple :class:`~parsl.concurrent.ParslPoolExecutors`,
each mapped to different types of Parsl workers (also called "executors").
Start by starting DFK with a Parsl Config that includes :ref:`multiple executors <config-multiple>`.
Then create separate executors with the same DFK but
different lists of executors their tasks will use.

.. code-block:: python

    with parsl.load(hybrid_config) as dfk, \
            ParslPoolExecutor(dfk=dfk, executors=['gpu']) as pool_gpu, \
            ParslPoolExecutor(dfk=dfk, executors=['cpu']) as pool_cpu:
        ...

Using a ParslPoolExecutor
-------------------------

The :class:`~parsl.concurrent.ParslPoolExecutor` supports all functions from
the :class:`~concurrent.futures.Executor` interface *except task cancellation*.

The  ``submit`` and ``map`` functions behave just as in :class:`~concurrent.futures.ProcessPoolExecutor`,
and also include the task chaining supported in App-based Parsl workflows.

.. code-block:: python

    from parsl.concurrent import ParslPoolExecutor
    from parsl.config.htex_local import config  # Mimicks the PythonPool

    def f(x):
        return x + 1

    with ParslPoolExecutor(config=config) as pool:
        # Submit a task then a task which depends on the result
        future_1 = pool.submit(f, 1)
        future_2 = pool.submit(f, future_1)

        assert future_1.result() == 2
        assert future_2.result() == 3

Tasks from the Executor and App-based interface may also be used together.

    .. code-block:: python

        def f(x):
            return x + 1

        @python_app
        def parity(x):
            return 'odd' if x % 2 == 1 else 'even'

        with ParslPoolExecutor(config=my_parsl_config) as executor:
            future_1 = executor.submit(f, 1)
            assert parity(future_1).result() == 'even'  # Function chaining, as expected


Differences
-----------

The differences between the Parsl-based :class:`~parsl.concurrent.ParslPoolExecutor`
and the Python :class:`~concurrent.futures.ProcessPoolExecutor` are:

1. *Task Cancellation*: Parsl does not support canceling tasks once submitted.

2. *Defining Functions*: Functions defined in modules work the same in both
   Parsl and the ``ProcessPoolExecutor``. However, those defined during execution
   (e.g., in the "main" file) behave differently.

   Parsl will serialize functions defined at runtime but they will not be able
   to access global variables (as is the case when using the
   `spawn start method <https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods>`_
   in the ``ProcessPoolExecutor``), which means modules must be imported
   inside the function.
   Follow the guidelines :ref:`in the App guide <function-rules>`.

3. *No Multiprocessing Objects*: Tools such as the :class:`multiprocessing.Queue` and
   `synchronization primitives <https://docs.python.org/3/library/multiprocessing.html#synchronization-primitives>`_
   may not work in Parsl without specific configuration of the :class:`multiprocessing.Manager`.

4. *Worker Initialization*: The worker initialization functions from :class:`~concurrent.futures.ProcessPoolExecutor`
   are not supported. Configure workers using the ``Config`` object intead.
