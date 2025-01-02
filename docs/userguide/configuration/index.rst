.. _configuration-section:

Configuring Parsl
=================

Parsl separates program logic from execution configuration, enabling
programs to be developed entirely independently from their execution
environment. Configuration is described by a Python object (:class:`~parsl.config.Config`) 
so that developers can 
introspect permissible options, validate settings, and retrieve/edit
configurations dynamically during execution. A configuration object specifies 
details of the provider, executors, allocation size,
queues, durations, and data management options. 

The following example shows a basic configuration object (:class:`~parsl.config.Config`) for the Frontera
supercomputer at TACC.
This config uses the `parsl.executors.HighThroughputExecutor` to submit
tasks from a login node. It requests an allocation of
128 nodes, deploying 1 worker for each of the 56 cores per node, from the normal partition.
To limit network connections to just the internal network the config specifies the address
used by the infiniband interface with ``address_by_interface('ib0')``

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


.. toctree::
    :maxdepth: 2

    execution
    elasticity
    pinning
    data
    heterogeneous
    encryption
    examples
