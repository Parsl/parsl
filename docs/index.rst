Parsl - Parallel Scripting Library
##################################

Parsl is parallel programming library for Python for parallelism beyond a single computer.

At the easiest level, you can use Parsl to run functions just like Python's executors but across *multiple cores and nodes*:

.. code-block:: python

    from parsl.concurrent import ParslPoolExecutor
    from parsl.configs.htex_local import config

    with ParslPoolExecutor(config) as exec:
        results = pool.map(function, [...])

Start with the `configuration quickstart <quickstart.html#getting-started>`_ to learn how to tell Parsl how to use your computing resource.
Parsl can run on most supercomputers and with major cloud providers.

The real power of Parsl comes is expressing multi-step workflows of functions.
Parsl lets you chain functions together and will launch each function as inputs and computing resources are available.

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

Why use Parsl?
++++++++++++++

Parsl is Python
---------------

*Everything about a Parsl program is written in Python.*
Parsl follows Python's native parallelization approach and functions,
how they combine into workflows, and where they run are all described in Python.

Parsl works everywhere
----------------------

*Parsl can run parallel functions on a laptop and the world's fastest supercomputers.*
Scaling from laptop to supercomputer is often as simple as changing the resource configuration.

Parsl is flexible
-----------------

..
    Is this redundant with 'everywhere'?

*Parsl supports many kinds of applications.*
Functions can be pure Python or invoke external codes, be single- or multi-threaded or GPUs.


Parsl handles data
------------------

*Parsl has first-class support for workflows involving files.*
Data will be automatically moved between workers, even if they reside on different filesystems.

Parsl is fast
-------------

*Parsl was built for speed.*
Workflows can manage tens of thousands of parallel tasks and process thousands of tasks per second.


Parsl is a community
--------------------

*Parsl is part of a large, experienced community.*

The Parsl Project was launched by researchers with decades of experience in workflows
as part of a National Science Foundation project to create sustainable research software.

The Parsl team is guided by the community through its GitHub,
conversations on `Slack <https://join.slack.com/t/parsl-project/shared_invite/enQtMzg2MDAwNjg2ODY1LTUyYmViOTY1ZTU2ZDUwZTVmOWQ1M2IzZTE5MTM3YWZlM2RhM2IyZjk4ODgwNGRhOGE1MzJlNTYyYjNkYzIzZWE>`_,
Bi-Weekly developer calls,
and engagement with the `Workflows Community Initiative <https://workflows.community/>`_.

..
    I was going to work in how we integrate with other tools, but that seemed too-detailed in this draft

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
