The WorkQueue Executor
======================

The :class:`parsl.executors.WorkQueueExecutor` (WQEx) executes Parsl tasks
using the `Work Queue, a framework from the Cooperative Computing Lab <https://ccl.cse.nd.edu/software/workqueue/>`_.
WQEx dispatches Parsl tasks to Work Queue's "managers" and places
the managers on to compute nodes via Parsl's `Providers <./providers/index.html>`_.

This part of the documentation details the unique capabilities available through Work Queue.

.. contents::
   :local:
   :depth: 1


Heterogeneous Resources
-----------------------

The :class:`parsl.executors.WorkQueueExecutor` provides several features to work with heterogeneous resources.
By default, Parsl only runs one app at a time on each worker node.
However, it is possible to specify the requirements for a particular app,
and Work Queue will automatically run as many parallel instances as possible on each node.
Work Queue automatically detects the amount of cores, memory, and other resources available on each execution node.
To activate this feature, add a resource specification to your apps. A resource specification is a dictionary with
the following three keys: ``cores`` (an integer corresponding to the number of cores required by the task),
``memory`` (an integer corresponding to the task's memory requirement in MB), and ``disk`` (an integer corresponding to
the task's disk requirement in MB), passed to an app via the special keyword argument ``parsl_resource_specification``. The specification can be set for all app invocations via a default, for example:

   .. code-block:: python

      @python_app
      def compute(x, parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
          return x*2


or updated when the app is invoked:

   .. code-block:: python

      spec = {'cores': 1, 'memory': 500, 'disk': 500}
      future = compute(x, parsl_resource_specification=spec)

This ``parsl_resource_specification`` special keyword argument will inform Work Queue about the resources this app requires.
When placing instances of ``compute(x)``, Work Queue will run as many parallel instances as possible based on each worker node's available resources.

If an app's resource requirements are not known in advance,
Work Queue has an auto-labeling feature that measures the actual resource usage of your apps and automatically chooses resource labels for you.
With auto-labeling, it is not necessary to provide ``parsl_resource_specification``;
Work Queue collects stats in the background and updates resource labels as your workflow runs.
To activate this feature, add the following flags to your executor config:

   .. code-block:: python

      config = Config(
          executors=[
              WorkQueueExecutor(
                  # ...other options go here
                  autolabel=True,
                  autocategory=True
              )
          ]
      )

The ``autolabel`` flag tells Work Queue to automatically generate resource labels.
By default, these labels are shared across all apps in your workflow.
The ``autocategory`` flag puts each app into a different category,
so that Work Queue will choose separate resource requirements for each app.
This is important if e.g. some of your apps use a single core and some apps require multiple cores.
Unless you know that all apps have uniform resource requirements,
you should turn on ``autocategory`` when using ``autolabel``.

Software Environment Detection
------------------------------

The Work Queue executor can also help deal with sites that have non-uniform software environments across nodes.
Parsl assumes that the Parsl program and the compute nodes all use the same Python version.
In addition, any packages your apps import must be available on compute nodes.
If no shared filesystem is available or if node configuration varies,
this can lead to difficult-to-trace execution problems.

If your Parsl program is running in a Conda environment,
the Work Queue executor can automatically scan the imports in your apps,
create a self-contained software package,
transfer the software package to worker nodes,
and run your code inside the packaged and uniform environment.
First, make sure that the Conda environment is active and you have the required packages installed (via either ``pip`` or ``conda``):

- ``python``
- ``parsl``
- ``ndcctools``
- ``conda-pack``

Then add the following to your config:

   .. code-block:: python

      config = Config(
          executors=[
              WorkQueueExecutor(
                  # ...other options go here
                  pack=True
              )
          ]
      )

.. note::
   There will be a noticeable delay the first time Work Queue sees an app;
   it is creating and packaging a complete Python environment.
   This packaged environment is cached, so subsequent app invocations should be much faster.

Using this approach, it is possible to run Parsl applications on nodes that don't have Python available at all.
The packaged environment includes a Python interpreter,
and Work Queue does not require Python to run.

.. note::
   The automatic packaging feature only supports packages installed via ``pip`` or ``conda``.
   Importing from other locations (e.g. via ``$PYTHONPATH``) or importing other modules in the same directory is not supported.
