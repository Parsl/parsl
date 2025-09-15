.. _configuration-section:

Configuring Parsl
=================

.. note::

    This part of the guide describes the how to configure Parsl from the start.
    Consider starting with `example configurations <examples.html>`_ to
    see if there is already an example which matches your needs.

Contemporary computing environments offer a wide range of computational platforms
or **execution providers**, from laptops and PCs to various clusters, supercomputers, and cloud computing platforms.
Different execution providers use of different **execution models**, such as threads (for efficient parallel execution on a multicore processor), processes, and pilot jobs for running many small tasks on a large parallel system.
Parsl is designed to abstract these low-level details so that an identical Parsl program can run unchanged on different platforms or across multiple platforms.
The Parsl configuration object (:class:`~parsl.config.Config`) specifies
which execution provider(s) and execution model(s) to use.

:class:`~parsl.config.Config` objects are expressed in Python
so that software can
introspect permissible options, validate settings, and retrieve/edit
configurations dynamically during execution.

The :class:`~parsl.config.Config` object, in short, is a list of **Executors**.
Each executor represents a different type of **worker** that will run Parsl tasks
and is defined by at least three components:

- *Worker Definition* which controls how workers are deployed on a resource.
  Options typically include how many workers per node and
  how many resources (e.g., CPU cores) each worker is a allowed to use.
- *Provider Description* which specifies how Parsl gains access to resources,
  such as by requesting nodes from a cluster's queuing system.
  Many types of providers require the name of a queue, how many nodes to request from the queue,
  and how long to request the nodes for.
- *Launcher Specification* which defines how the workers which execute Parsl tasks
  are placed on to compute nodes.

Build a :class:`~parsl.config.Config` by first learning about
the `configuration object <config.html>`_ then
the types of `Executors <executors/index.html>`_ available in Parsl, which
each may require using a `resource provider and launcher <providers/index.html>`_.

Alternatively, find an `example configuration <examples.html>`_ which is similar to what you need.

.. toctree::
    :maxdepth: 2

    config
    executors/index
    providers/index
    examples
