.. _configuration-section:

Configuring Parsl
=================

.. note::

    This part of the guide describes the how to configure Parsl from the start.
    Consider starting with `example configurations <examples.html>`_ to
    see if there is already an example which matches your needs.

Parsl configuration objects (:class:`~parsl.config.Config`) define
how to acquire resources to execute tasks,
how to launch workers on the resources,
and how many resources each worker has.

:class:`~parsl.config.Config` objects are expressed in Python
so that software can
introspect permissible options, validate settings, and retrieve/edit
configurations dynamically during execution.

The :class:`~parsl.config.Config` object, in short, is a list of **Executors**.
Each executor represents a different type of worker Parsl will use to run tasks
and an executor contains at least three components:

- *Worker Definition* which controls how many workers are placed on each node..
  Options typically include how many workers per node and
  how many resources (e.g., CPU cores) each worker is a allowed to use.
- *Provider Description* which specifies how Parsl gains access to resources,
  such as by requesting nodes from a cluster's queuing system.
  Many types of provides require the name of a queue, how many nodes to request from the queue,
  and how long to request the nodes for.
- *Launcher Specification* which defines how the workers which execute Parsl tasks
  are placed on to the compute node.

Learn to build a :class:`~parsl.config.Config` by first learning about
the `configuration object <config.html>`_ then
the types of `Executors <executors/index.html>`_ available in Parsl, which
each may require using a `resource provider and launcher <providers/index.html>`_.

Alternatively, start by finding an `example configuration <examples.html>`_ which is similar to what you need.

.. toctree::
    :maxdepth: 2

    config
    executors/index
    providers/index
    examples
