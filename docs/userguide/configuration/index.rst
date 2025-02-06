.. _configuration-section:

Configuring Parsl
=================

Parsl configuration objects (:class:`~parsl.config.Config`) define
how to acquire resources to execute tasks,
how to launch workers on the resources,
and how many resources each worker has.

:class:`~parsl.config.Config` objects are expressed in Python
so that software can
introspect permissible options, validate settings, and retrieve/edit
configurations dynamically during execution.

The main part of the configuration objects is a list of
**executors** which each define a certain type of worker
for executing Parsl tasks.
A single application can have multiple executors,
which is often used when the application uses tasks
with different compute requirements.

Anatomy of an Executor
----------------------

Most Parsl executors are defined by three components:

- *Worker Definition* which controls how many workers are placed on each node..
  Options typically include how many workers per node, and
  :ref:`how each worker is mapped to CPUs or accelerators <affinity>`.
- *Provider Description* which specifies how Parsl gains access to resources,
  such as by requesting nodes from a cluster's queuing system.
  Many types of provides require the name of a queue, how many nodes to request from the queue,
  and how long to request the nodes for.
- *Launcher Specification* which defines how the workers which execute Parsl tasks
  are placed on to the compute node.

The following pages of the Parsl user guide will explain all three classes of components.

A Simple Example
++++++++++++++++

The following example of a :class:`~parsl.config.Config` shows the major configuration options.

.. code-block:: python

    # Example Config for Parsl on TACC's Frontera supercomputer
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

Subsections
-----------

The following sections detail the many configuration options available in Parsl.

.. toctree::
    :maxdepth: 2

    execution
    elasticity
    pinning
    data
    heterogeneous
    encryption
    examples
