Config Objects
==============

A :class:`~parsl.config.Config` object defines and implements how Parsl connects
to compute resources used to execute tasks.

The main part of the ``Config`` is a list of
**executors** which each define a type of worker.

Consider the following example, a configuration to run 7168 parallel workers on
the Frontera supercomputer at TACC:

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher
    from parsl.addresses import address_by_interface

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="frontera_htex",
                address=address_by_interface('ib0'),
                max_workers_per_node=56,
                provider=SlurmProvider(
                    partition='normal',
                    nodes_per_block=128,
                    init_blocks=1,
                    launcher=SrunLauncher(),
                ),
            )
        ],
    )


The worker options are those for :class:`~parsl.executors.HighThroughputExecutor`,
the baseline Executor available in Parsl.
The options include a name for the workers of this type (``label``),
how the worker connects to the main Parsl process (``address``),
and how many workers to place on each compute node (``max_workers_per_node``).

The provider options define the queue used for submission (``partition``),
and tells Parsl request one job (``init_blocks``) of 128 nodes (``nodes_per_block``).

The launcher uses the default mechanism for starting programs on Frontera compute nodes, ``srun``.

Using a Config Object
---------------------

Use the ``Config`` object to start Parsl's data flow kernel with the ``parsl.load`` method:

.. code-block:: python

    from parsl.configs.htex_local import config
    import parsl

    with parsl.load(config) as dfk:
        future = app(x)
        future.wait()

The ``.load()`` function creates a DataFlowKernel ("DFK") object that maintains the workflows state.
The DFK acquires the resources used by Parsl and should be closed to release the resources.
While the DFK can be closed manually (``dfk.cleanup()``), the preferred and Pythonic route is use it as a context manager (the ``with`` statement)
Using a context manager avoids unnecessary code and ensures cleanup occurs even if the workflow fails.

The ``load`` statement can happen after Apps are defined but must occur before tasks are started.
Loading the Config object within context manager like ``with`` is recommended
for implicit cleaning of DFK on exiting the context manager.

The :class:`~parsl.config.Config` object may not be used again once loaded.
Consider a configuration function if the application will shut down and re-launch the DFK.

.. code-block:: python

    from parsl.config import Config
    import parsl

    def make_config() -> Config:
        return Config(...)

    with parsl.load(make_config()):
        # Your workflow here

    # Section which does not require Parsl

    with parsl.load(make_config()):
        # Another workflow here


Config Options
--------------

Options for the :class:`~parsl.config.Config` object apply to Parsl's general behavior
and affect all executors.
Common options include:

- ``run_dir`` for setting where Parsl writes log files
- ``retries`` to restart failed tasks
- ``usage_tracking`` to help Parsl `by reporting how you use it <../advanced/usage_tracking.html>`_

Consult the :py:class:`API documentation for Config <parsl.config.Config>`
or the `advanced documentation <../advanced/index.html>`_ to learn about options.

Multiple Executors
------------------

A single application can configure multiple executors.

All executors define a ``label`` field that is used
route to specific workers.
All types of apps include a ``executors`` option which takes
a list of executor labels.
For example, tasks from the following App will only run on an executor labelled "frontera_htex".

.. code-block:: python

    @python_app(executors=['frontera_htex'])
    def single_threaded_task(x: int):
        return x * 2 + 1


Consider using multiple executors in the following cases:

- *Different resource requirements between tasks*, such as a workflow
  with a simulation stage that runs on the CPU nodes of an HPC system
  followed by an analysis and visualization stage that runs on GPU nodes.
- *Different scales between workflow stages*, such as a workflow
  with a "fan-out" stage of many long running running on a cluster
  and quick "fan-in" computations which can run on fewer nodes.
