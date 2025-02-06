Config Objects
==============

A :class:`~parsl.config.Config` objects defines and implements how Parsl connects
resources that execute its task

The main part of the configuration is a list of
**executors** which each define a certain type of worker
for executing Parsl tasks.

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
                    nodes_per_block=128,
                    init_blocks=1,
                    partition='normal',
                    launcher=SrunLauncher(),
                ),
            )
        ],
    )


The worker options are those for :class:`~parsl.executors.HighThroughputExecutor`.
The options include a name for the workers of this type (``label``),
how the worker connects to the main Parsl process (``address``),
and how many workers to place on each compute node (``max_workers_per_node``).

The provider options are define the queue used for submission (``partition``)
and sets Parsl to request one job (``init_blocks``) of 128 nodes (``nodes_per_block``).

The launcher uses the default mechanism for starting programs on Frontera compute nodes, ``srun``.

Using a Config Object
---------------------

Use the ``Config`` object to start Parsl's data flow kernel with the ``parsl.load`` method :

.. code-block:: python

    from parsl.configs.htex_local import config
    import parsl

    with parsl.load(config):

The ``load`` statement can happen after Apps are defined but must occur before tasks are started.
Loading the Config object within context manager like ``with`` is recommended
for implicit cleaning of DFK on exiting the context manager.

The :class:`~parsl.config.Config` object may not be used again after loaded.
Consider a configuration function if the application will shut down and re-launch the DFK.

.. code-block:: python

    from parsl.config import Config
    import parsl

    def make_config() -> Config:
        return Config(...)

    with parsl.load(make_config()):
        # Your workflow here
    parsl.clear()  # Stops Parsl
    with parsl.load(make_config()):  # Re-launches with a fresh configuration
        # Your workflow here

Multiple Executors
------------------

A single application can configure multiple executors,
which is often used when the application uses tasks
with different compute requirements.

All executors define a ``label`` field that is used
in log files and when determining where to run different tasks.

Tasks from the following app will only run on an executor labelled "frontera_htex".

.. code-block:: python

    @python_app(executors=['frontera_htex'])
    def single_threaded_task(x: int):
        return x * 2 + 1


Tasks run on all available executors by default.

Consider using multiple executors in the following cases:

The common scenarios for this feature are:

- *Different resource requirements between tasks*, such as a workflow
  with a simulation stage that runs on the CPU nodes of an HPC system
  followed by an analysis and visualization stage that runs on GPU nodes.
* *Different scales between workflow stages*, such as a workflow
  with a "fan-out" stage of many long running running on a cluster
  and quick "fan-in" computations which can run on the login node.
