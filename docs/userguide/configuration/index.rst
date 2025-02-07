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

.. toctree::
    :maxdepth: 2

    config
    executors/index
    execution
    elasticity
    heterogeneous
    examples
