Quickstart
==========

To try Parsl now (without installing any code locally), experiment with our 
`hosted tutorial notebooks on Binder <https://mybinder.org/v2/gh/Parsl/parsl-tutorial/master>`_.


Installation
------------

Parsl is available on `PyPI <https://pypi.org/project/parsl/>`_ and `conda-forge <https://anaconda.org/conda-forge/parsl>`_. 

Parsl requires Python3.7+ and has been tested on Linux and macOS.


Installation using Pip
^^^^^^^^^^^^^^^^^^^^^^

While ``pip`` can be used to install Parsl, we suggest the following approach
for reliable installation when many Python environments are available.

1. Install Parsl::

     $ python3 -m pip install parsl

To update a previously installed parsl to a newer version, use: ``python3 -m pip install -U parsl``


Installation using Conda
^^^^^^^^^^^^^^^^^^^^^^^^

1. Create and activate a new conda environment::

     $ conda create --name parsl_py37 python=3.7
     $ source activate parsl_py37

2. Install Parsl::

     $ python3 -m pip install parsl

     or

     $ conda config --add channels conda-forge
     $ conda install parsl


The conda documentation provides `instructions <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_ for installing conda on macOS and Linux. 

Getting started
---------------

Parsl has much in common with Python's native concurrency library,
but unlocking Parsl's potential requires understanding a few major concepts.

A Parsl program submits tasks to run on Workers distributed across remote computers.
The instructions for these tasks are contained within `"apps" <#application-types>`_
that users define using Python functions.
Each remote computer (e.g., a node on a supercomputer) has a single `"Executor" <#executors>`_
which manages the workers.
Remote resources available to Parsl are acquired by a `"Provider" <#resource-providers>`_,
which places the executor on a system with a `"Launcher" <#launchers>`_.
Task execution is brokered by a `"Data Flow Kernel" <#benefits-of-a-data-flow-kernel>`_ that runs on your local system.

We describe these components briefly here, and link to more details in the `User Guide <userguide/index.html>`_.

Application Types
^^^^^^^^^^^^^^^^^

Parsl enables concurrent execution of Python functions (``python_app``)
or external applications (``bash_app``).
The logic for both are described by Python functions marked with with Parsl decorators.
When decorated functions are invoked, they run asynchronously on other resources.
The result of a call to a Parsl app is an :class:`~parsl.app.futures.AppFuture`,
which behaves like a Python Future.

The following example shows how to write a simple Parsl program
with hello world Python and Bash apps.

.. code-block:: python

    import parsl
    from parsl import python_app, bash_app

    parsl.load()

    @python_app
    def hello_python (message):
        return 'Hello %s' % message

    @bash_app
    def hello_bash(message, stdout='hello-stdout'):
        return 'echo "Hello %s"' % message

    # invoke the Python app and print the result
    print(hello_python('World (Python)').result())

    # invoke the Bash app and read the result from a file
    hello_bash('World (Bash)').result()

    with open('hello-stdout', 'r') as f:
        print(f.read())

Learn more about the types of Apps and their options `here <userguide/apps.html>`_.

Executors
^^^^^^^^^

Executors define how Parsl deploys work on a computer.
Many types are available, each with different advantages.

The :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` is most familiar to most people.
Like Python's ``ProcessPoolExecutor``, the workers it creates are separate Python processes.
However, you have much more control over how the work is deployed.
You can dynamically set the number of workers based on available memory and
pin each worker to specific GPUs or CPU cores
among other powerful features.

Learn more about Executors `here <userguide/execution.html#executors>`_.

Execution Providers
^^^^^^^^^^^^^^^^^^^

Resource providers allow Parsl to gain access to computing power.
For supercomputers, gaining resources often requires requesting them from a scheduler (e.g., Slurm).
Parsl Providers write the requests to requisition **"Blocks"** of computers (e.g., supercomputer nodes) on your behalf.
Parsl comes pre-packaged with Providers compatible with most supercomputers and some cloud computing services.

Another key role of Providers is defining how to start an Executor on a remote computer.
Often, this simply involves specifying the correct Python environment and
(described below) how to launch the Executor on each acquired computers.

Learn more about Providers `here <userguide/execution.html#execution-providers>`_ and
find examples for common supercomputers `here <userguide/configuring.html>`_.

Launchers
^^^^^^^^^

The Launcher defines how to spread workers across all computers available in a Block.
A common example is an :class:`~parsl.launchers.launchers.MPILauncher`, which uses MPI's mechanism
for starting a single program on multiple computing nodes.
Like Providers, Parsl comes packaged with Launchers for most supercomputers and clouds.

Learn more about Launchers `here <userguide/execution.html#launchers>`_


Benefits of a Data-Flow Kernel
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Data-Flow Kernel (DFK) is the behind-the-scenes engine behind Parsl.
The DFK determines when tasks can be started and sends them to open resources,
receives results, restarts failed tasks, propagates errors to dependent tasks,
and performs the many other functions needed to execute complex workflows.
The flexibility and performance of the DFK enables applications with
intricate dependencies between tasks to execute on thousands of parallel workers.

Start with the Tutorial or the `parallel patterns <userguide/workflow.html>`_
to see the complex types of workflows you can make with Parsl,

Tutorial
--------

The best way to learn more about Parsl is by reviewing the Parsl tutorials.
There are several options for following the tutorial: 

1. Use `Binder <https://mybinder.org/v2/gh/Parsl/parsl-tutorial/master>`_  to follow the tutorial online without installing or writing any code locally. 
2. Clone the `Parsl tutorial repository <https://github.com/Parsl/parsl-tutorial>`_ using a local Parsl installation.
3. Read through the online `tutorial documentation <1-parsl-introduction.html>`_.


Usage Tracking
--------------

To help support the Parsl project, we ask that users opt-in to anonymized usage tracking
whenever possible. Usage tracking allows us to measure usage, identify bugs, and improve
usability, reliability, and performance. Only aggregate usage statistics will be used
for reporting purposes. 

As an NSF-funded project, our ability to track usage metrics is important for continued funding. 

You can opt-in by setting ``PARSL_TRACKING=true`` in your environment or by 
setting ``usage_tracking=True`` in the configuration object (`parsl.config.Config`). 

To read more about what information is collected and how it is used see :ref:`label-usage-tracking`.


For Developers
--------------

Parsl is an open source community that encourages contributions from users
and developers. A guide for `contributing <https://github.com/Parsl/parsl/blob/master/CONTRIBUTING.rst>`_ 
to Parsl is available in the `Parsl GitHub repository <https://github.com/Parsl/parsl>`_.

The following instructions outline how to set up Parsl from source.

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl

2. Install::

    $ cd parsl
    $ pip install .
    ( To install specific extra options from the source :)
    $ pip install '.[<optional_package1>...]'

3. Use Parsl!

