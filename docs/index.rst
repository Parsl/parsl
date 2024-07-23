Parsl - Parallel Scripting Library
##################################

Parsl extends parallelism in Python beyond a single computer.

You can use Parsl
`just like Python's parallel executors <userguide/workflow.html#parallel-workflows-with-loops>`_
but across *multiple cores and nodes*.
However, the real power of Parsl is in expressing multi-step workflows of functions.
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
    def g(x, y):
        return x + y

    # These functions now return Futures, and can be chained
    future = f(1)
    assert future.result() == 2

    # Functions run concurrently, can be chained
    f_a, f_b = f(2), f(3)
    future = g(f_a, f_b)
    assert future.result() == 7


Start with the `configuration quickstart <quickstart.html#getting-started>`_ to learn how to tell Parsl how to use your computing resource,
see if `a template configuration for your supercomputer <userguide/configuring.html>`_ is already available,
then explore the `parallel computing patterns <userguide/workflow.html>`_ to determine how to use parallelism best in your application.

Parsl is an open-source code, and available on GitHub: https://github.com/parsl/parsl/

Why use Parsl?
==============

Parsl is Python
---------------

*Everything about a Parsl program is written in Python.*
Parsl follows Python's native parallelization approach and functions,
how they combine into workflows, and where they run are all described in Python.

Parsl works everywhere
----------------------

*Parsl can run parallel functions on a laptop and the world's fastest supercomputers.*
Scaling from laptop to supercomputer is often as simple as changing the resource configuration.
Parsl is tested `on many of the top supercomputers <userguide/configuring.html>`_.

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
   beginnerguide/index
   userguide/index
   faq
   reference
   devguide/index
   historical/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
