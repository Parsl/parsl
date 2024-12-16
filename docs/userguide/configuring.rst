.. _configuration-section:

Configuration
=============

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

.. contents:: Configuration How-To and Examples:


Creating and Using Config Objects
---------------------------------

:class:`~parsl.config.Config` objects are loaded to define the "Data Flow Kernel" (DFK) that will manage tasks.
All Parsl applications start by creating or importing a configuration then calling the load function.

.. code-block:: python

    from parsl.configs.htex_local import config
    import parsl

    with parsl.load(config):

The ``load`` statement can happen after Apps are defined but must occur before tasks are started.
Loading the Config object within context manager like ``with`` is recommended
for implicit cleaning of DFK on exiting the context manager  

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


How to Configure
----------------

.. note::
   All configuration examples below must be customized for the user's
   allocation, Python environment, file system, etc.


The configuration specifies what, and how, resources are to be used for executing
the Parsl program and its apps.
It is important to carefully consider the needs of the Parsl program and its apps,
and the characteristics of the compute resources, to determine an ideal configuration. 
Aspects to consider include:
1) where the Parsl apps will execute;
2) how many nodes will be used to execute the apps, and how long the apps will run;
3) should Parsl request multiple nodes in an individual scheduler job; and
4) where will the main Parsl program run and how will it communicate with the apps.

Stepping through the following question should help formulate a suitable configuration object.

1. Where should apps be executed?

+---------------------+-----------------------------------------------+----------------------------------------+
| Target              | Executor                                      | Provider                               |
+=====================+===============================================+========================================+
| Laptop/Workstation  | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.LocalProvider`        |
|                     | * `parsl.executors.ThreadPoolExecutor`        |                                        |
|                     | * `parsl.executors.WorkQueueExecutor`         |                                        |
|                     | * `parsl.executors.taskvine.TaskVineExecutor` |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Amazon Web Services | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.AWSProvider`          |
+---------------------+-----------------------------------------------+----------------------------------------+
| Google Cloud        | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.GoogleCloudProvider`  |
+---------------------+-----------------------------------------------+----------------------------------------+
| Slurm based system  | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.SlurmProvider`        |
|                     | * `parsl.executors.WorkQueueExecutor`         |                                        |
|                     | * `parsl.executors.taskvine.TaskVineExecutor` |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Torque/PBS based    | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.TorqueProvider`       |
| system              | * `parsl.executors.WorkQueueExecutor`         |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| GridEngine based    | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.GridEngineProvider`   |
| system              | * `parsl.executors.WorkQueueExecutor`         |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Condor based        | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.CondorProvider`       |
| cluster or grid     | * `parsl.executors.WorkQueueExecutor`         |                                        |
|                     | * `parsl.executors.taskvine.TaskVineExecutor` |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Kubernetes cluster  | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.KubernetesProvider`   |
+---------------------+-----------------------------------------------+----------------------------------------+


2.  How many nodes will be used to execute the apps? What task durations are necessary to achieve good performance?


+--------------------------------------------+----------------------+-------------------------------------+
| Executor                                   | Number of Nodes [*]_ | Task duration for good performance  |
+============================================+======================+=====================================+
| `parsl.executors.ThreadPoolExecutor`       | 1 (Only local)       | Any                                 |
+--------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.HighThroughputExecutor`   | <=2000               | Task duration(s)/#nodes >= 0.01     |
|                                            |                      | longer tasks needed at higher scale |
+--------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.WorkQueueExecutor`        | <=1000 [*]_          | 10s+                                |
+--------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.taskvine.TaskVineExecutor`| <=1000 [*]_          | 10s+                                |
+--------------------------------------------+----------------------+-------------------------------------+


.. [*] Assuming 32 workers per node. If there are fewer workers launched
       per node, a larger number of nodes could be supported.

.. [*] The maximum number of nodes tested for the `parsl.executors.WorkQueueExecutor` is 10,000 GPU cores and
       20,000 CPU cores.

.. [*] The maximum number of nodes tested for the `parsl.executors.taskvine.TaskVineExecutor` is 
       10,000 GPU cores and 20,000 CPU cores.

3. Should Parsl request multiple nodes in an individual scheduler job? 
(Here the term block is equivalent to a single scheduler job.)

+--------------------------------------------------------------------------------------------+
| ``nodes_per_block = 1``                                                                    |
+---------------------+--------------------------+-------------------------------------------+
| Provider            | Executor choice          | Suitable Launchers                        |
+=====================+==========================+===========================================+
| Systems that don't  | Any                      | * `parsl.launchers.SingleNodeLauncher`    |
| use Aprun           |                          | * `parsl.launchers.SimpleLauncher`        |
+---------------------+--------------------------+-------------------------------------------+
| Aprun based systems | Any                      | * `parsl.launchers.AprunLauncher`         |
+---------------------+--------------------------+-------------------------------------------+

+---------------------------------------------------------------------------------------------------------------------+
| ``nodes_per_block > 1``                                                                                             |
+-------------------------------------+--------------------------+----------------------------------------------------+
| Provider                            | Executor choice          | Suitable Launchers                                 |
+=====================================+==========================+====================================================+
| `parsl.providers.TorqueProvider`    | Any                      | * `parsl.launchers.AprunLauncher`                  |
|                                     |                          | * `parsl.launchers.MpiExecLauncher`                |
+-------------------------------------+--------------------------+----------------------------------------------------+
| `parsl.providers.SlurmProvider`     | Any                      | * `parsl.launchers.SrunLauncher`  if native slurm  |
|                                     |                          | * `parsl.launchers.AprunLauncher`, otherwise       |
+-------------------------------------+--------------------------+----------------------------------------------------+

.. note:: If using a Cray system, you most likely need to use the `parsl.launchers.AprunLauncher` to launch workers unless you
          are on a **native Slurm** system like :ref:`configuring_nersc_cori`


Heterogeneous Resources
-----------------------

In some cases, it can be difficult to specify the resource requirements for running a workflow.
For example, if the compute nodes a site provides are not uniform, there is no "correct" resource configuration;
the amount of parallelism depends on which node (large or small) each job runs on.
In addition, the software and filesystem setup can vary from node to node.
A Condor cluster may not provide shared filesystem access at all,
and may include nodes with a variety of Python versions and available libraries.

The `parsl.executors.WorkQueueExecutor` provides several features to work with heterogeneous resources.
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


Accelerators
------------

Many modern clusters provide multiple accelerators per compute note, yet many applications are best suited to using a
single accelerator per task. Parsl supports pinning each worker to different accelerators using
``available_accelerators`` option of the :class:`~parsl.executors.HighThroughputExecutor`. Provide either the number of
executors (Parsl will assume they are named in integers starting from zero) or a list of the names of the accelerators
available on the node. Parsl will limit the number of workers it launches to the number of accelerators specified,
in other words, you cannot have more workers per node than there are accelerators. By default, Parsl will launch
as many workers as the accelerators specified via ``available_accelerators``.

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                available_accelerators=2,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

It is possible to bind multiple/specific accelerators to each worker by specifying a list of comma separated strings
each specifying accelerators. In the context of binding to NVIDIA GPUs, this works by setting ``CUDA_VISIBLE_DEVICES``
on each worker to a specific string in the list supplied to ``available_accelerators``.

Here's an example:

.. code-block:: python

    # The following config is trimmed for clarity
    local_config = Config(
        executors=[
            HighThroughputExecutor(
                # Starts 2 workers per node, each bound to 2 GPUs
                available_accelerators=["0,1", "2,3"],

                # Start a single worker bound to all 4 GPUs
                # available_accelerators=["0,1,2,3"]
            )
        ],
    )

GPU Oversubscription
""""""""""""""""""""

For hardware that uses Nvidia devices, Parsl allows for the oversubscription of workers to GPUS.  This is intended to
make use of Nvidia's `Multi-Process Service (MPS) <https://docs.nvidia.com/deploy/mps/>`_ available on many of their
GPUs that allows users to run multiple concurrent processes on a single GPU.  The user needs to set in the
``worker_init`` commands to start MPS on every node in the block (this is machine dependent).  The
``available_accelerators`` option should then be set to the total number of GPU partitions run on a single node in the
block.  For example, for a node with 4 Nvidia GPUs, to create 8 workers per GPU, set ``available_accelerators=32``.
GPUs will be assigned to workers in ascending order in contiguous blocks.  In the example, workers 0-7 will be placed
on GPU 0, workers 8-15 on GPU 1, workers 16-23 on GPU 2, and workers 24-31 on GPU 3.
    
Multi-Threaded Applications
---------------------------

Workflows which launch multiple workers on a single node which perform multi-threaded tasks (e.g., NumPy, Tensorflow operations) may run into thread contention issues.
Each worker may try to use the same hardware threads, which leads to performance penalties.
Use the ``cpu_affinity`` feature of the :class:`~parsl.executors.HighThroughputExecutor` to assign workers to specific threads.  Users can pin threads to 
workers either with a strategy method or an explicit list.

The strategy methods will auto assign all detected hardware threads to workers.  
Allowed strategies that can be assigned to ``cpu_affinity`` are ``block``, ``block-reverse``, and ``alternating``.  
The ``block`` method pins threads to workers in sequential order (ex: 4 threads are grouped (0, 1) and (2, 3) on two workers);
``block-reverse`` pins threads in reverse sequential order (ex: (3, 2) and (1, 0)); and ``alternating`` alternates threads among workers (ex: (0, 2) and (1, 3)).

Select the best blocking strategy for processor's cache hierarchy (choose ``alternating`` if in doubt) to ensure workers to not compete for cores.

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                cpu_affinity='alternating',
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

Users can also use ``cpu_affinity`` to assign explicitly threads to workers with a string that has the format of 
``cpu_affinity="list:<worker1_threads>:<worker2_threads>:<worker3_threads>"``.

Each worker's threads can be specified as a comma separated list or a hyphenated range:
``thread1,thread2,thread3``
or
``thread_start-thread_end``.

An example for 12 workers on a node with 208 threads is:

.. code-block:: python

    cpu_affinity="list:0-7,104-111:8-15,112-119:16-23,120-127:24-31,128-135:32-39,136-143:40-47,144-151:52-59,156-163:60-67,164-171:68-75,172-179:76-83,180-187:84-91,188-195:92-99,196-203"

This example assigns 16 threads each to 12 workers. Note that in this example there are threads that are skipped.  
If a thread is not explicitly assigned to a worker, it will be left idle.
The number of thread "ranks" (colon separated thread lists/ranges) must match the total number of workers on the node; otherwise an exception will be raised.



Thread affinity is accomplished in two ways.
Each worker first sets the affinity for the Python process using `the affinity mask <https://docs.python.org/3/library/os.html#os.sched_setaffinity>`_,
which may not be available on all operating systems.
It then sets environment variables to control 
`OpenMP thread affinity <https://hpc-tutorials.llnl.gov/openmp/ProcessThreadAffinity.pdf>`_
so that any subprocesses launched by a worker which use OpenMP know which processors are valid.
These include ``OMP_NUM_THREADS``, ``GOMP_COMP_AFFINITY``, and ``KMP_THREAD_AFFINITY``.

Ad-Hoc Clusters
---------------

Parsl's support of ad-hoc clusters of compute nodes without a scheduler
is deprecated.

See
`issue #3515 <https://github.com/Parsl/parsl/issues/3515>`_
for further discussion.

Amazon Web Services
-------------------

.. image:: ./aws_image.png

.. note::
   To use AWS with Parsl, install Parsl with AWS dependencies via ``python3 -m pip install 'parsl[aws]'``

Amazon Web Services is a commercial cloud service which allows users to rent a range of computers and other computing services.
The following snippet shows how Parsl can be configured to provision nodes from the Elastic Compute Cloud (EC2) service.
The first time this configuration is used, Parsl will configure a Virtual Private Cloud and other networking and security infrastructure that will be
re-used in subsequent executions. The configuration uses the `parsl.providers.AWSProvider` to connect to AWS.

.. literalinclude:: ../../parsl/configs/ec2.py


ASPIRE 1 (NSCC)
---------------

.. image:: https://www.nscc.sg/wp-content/uploads/2017/04/ASPIRE1Img.png

The following snippet shows an example configuration for accessing NSCC's **ASPIRE 1** supercomputer. This example uses the `parsl.executors.HighThroughputExecutor` executor and connects to ASPIRE1's PBSPro scheduler. It also shows how ``scheduler_options`` parameter could be used for scheduling array jobs in PBSPro.

.. literalinclude:: ../../parsl/configs/ASPIRE1.py




Illinois Campus Cluster (UIUC)
------------------------------

.. image:: https://campuscluster.illinois.edu/wp-content/uploads/2018/02/ND2_3633-sm.jpg

The following snippet shows an example configuration for executing on the Illinois Campus Cluster.
The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface
with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/illinoiscluster.py

Bridges (PSC)
-------------

.. image:: https://insidehpc.com/wp-content/uploads/2016/08/Bridges_FB1b.jpg

The following snippet shows an example configuration for executing on the Bridges supercomputer at the Pittsburgh Supercomputing Center.
The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface
with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/bridges.py



CC-IN2P3
--------

.. image:: https://cc.in2p3.fr/wp-content/uploads/2017/03/bandeau_accueil.jpg

The snippet below shows an example configuration for executing from a login node on IN2P3's Computing Centre.
The configuration uses the `parsl.providers.LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support.
This system uses Grid Engine which Parsl interfaces with using the `parsl.providers.GridEngineProvider`.

.. literalinclude:: ../../parsl/configs/cc_in2p3.py


CCL (Notre Dame, TaskVine)
--------------------------

.. image:: https://ccl.cse.nd.edu/software/taskvine/taskvine-logo.png

To utilize TaskVine with Parsl, please install the full CCTools software package within an appropriate Anaconda or Miniconda environment
(instructions for installing Miniconda can be found `in the Conda install guide <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_):

.. code-block:: bash

   $ conda create -y --name <environment> python=<version> conda-pack
   $ conda activate <environment>
   $ conda install -y -c conda-forge ndcctools parsl

This creates a Conda environment on your machine with all the necessary tools and setup needed to utilize TaskVine with the Parsl library.

The following snippet shows an example configuration for using the Parsl/TaskVine executor to run applications on the local machine.
This examples uses the `parsl.executors.taskvine.TaskVineExecutor` to schedule tasks, and a local worker will be started automatically. 
For more information on using TaskVine, including configurations for remote execution, visit the 
`TaskVine/Parsl documentation online <https://cctools.readthedocs.io/en/latest/taskvine/#parsl>`_.

.. literalinclude::  ../../parsl/configs/vineex_local.py

TaskVine's predecessor, WorkQueue, may continue to be used with Parsl.
For more information on using WorkQueue visit the `CCTools documentation online <https://cctools.readthedocs.io/en/latest/help/>`_.

Expanse (SDSC)
--------------

.. image:: https://www.hpcwire.com/wp-content/uploads/2019/07/SDSC-Expanse-graphic-cropped.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer
Center's **Expanse** supercomputer. The example is designed to be executed on the login nodes, using the
`parsl.providers.SlurmProvider` to interface with the Slurm scheduler used by Comet and the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/expanse.py


Improv (Argonne LCRC)
---------------------

.. image:: https://www.lcrc.anl.gov/sites/default/files/styles/965_wide/public/2023-12/20231214_114057.jpg?itok=A-Rz5pP9

**Improv** is a PBS Pro based  supercomputer at Argonne's Laboratory Computing Resource
Center (LCRC). The following snippet is an example configuration that uses `parsl.providers.PBSProProvider`
and `parsl.launchers.MpiRunLauncher` to run on multinode jobs.

.. literalinclude:: ../../parsl/configs/improv.py


.. _configuring_nersc_cori:

Perlmutter (NERSC)
------------------

NERSC provides documentation on `how to use Parsl on Perlmutter <https://docs.nersc.gov/jobs/workflow/parsl/>`_.
Perlmutter is a Slurm based HPC system and parsl uses `parsl.providers.SlurmProvider` with `parsl.launchers.SrunLauncher`
to launch tasks onto this machine.


Frontera (TACC)
---------------

.. image:: https://frontera-portal.tacc.utexas.edu/media/filer_public/2c/fb/2cfbf6ab-818d-42c8-b4d5-9b39eb9d0a05/frontera-banner-home.jpg

Deployed in June 2019, Frontera is the 5th most powerful supercomputer in the world. Frontera replaces the NSF Blue Waters system at NCSA
and is the first deployment in the National Science Foundation's petascale computing program. The configuration below assumes that the user is
running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/frontera.py


Kubernetes Clusters
-------------------

.. image:: https://d1.awsstatic.com/PAC/kuberneteslogo.eabc6359f48c8e30b7a138c18177f3fd39338e05.png

Kubernetes is an open-source system for container management, such as automating deployment and scaling of containers.
The snippet below shows an example configuration for deploying pods as workers on a Kubernetes cluster.
The KubernetesProvider exploits the Python Kubernetes API, which assumes that you have kube config in ``~/.kube/config``.

.. literalinclude:: ../../parsl/configs/kubernetes.py


Midway (RCC, UChicago)
----------------------

.. image:: https://rcc.uchicago.edu/sites/rcc.uchicago.edu/files/styles/slideshow-image/public/uploads/images/slideshows/20140430_RCC_8978.jpg?itok=BmRuJ-wq

This Midway cluster is a campus cluster hosted by the Research Computing Center at the University of Chicago.
The snippet below shows an example configuration for executing remotely on Midway.
The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface
with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/midway.py


Open Science Grid
-----------------

.. image:: https://www.renci.org/wp-content/uploads/2008/10/osg_logo.png

The Open Science Grid (OSG) is a national, distributed computing Grid spanning over 100 individual sites to provide tens of thousands of CPU cores.
The snippet below shows an example configuration for executing remotely on OSG. You will need to have a valid project name on the OSG.
The configuration uses the `parsl.providers.CondorProvider` to interface with the scheduler.

.. literalinclude:: ../../parsl/configs/osg.py


Polaris (ALCF)
--------------

.. image:: https://www.alcf.anl.gov/sites/default/files/styles/965x543/public/2022-07/33181D_086_ALCF%20Polaris%20Crop.jpg?itok=HVAHsZtt
    :width: 75%

ALCF provides documentation on `how to use Parsl on Polaris <https://docs.alcf.anl.gov/polaris/workflows/parsl/>`_.
Polaris uses `parsl.providers.PBSProProvider` and `parsl.launchers.MpiExecLauncher` to launch tasks onto the HPC system.



Stampede2 (TACC)
----------------

.. image:: https://www.tacc.utexas.edu/documents/1084364/1413880/stampede2-0717.jpg/

The following snippet shows an example configuration for accessing TACC's **Stampede2** supercomputer. This example uses theHighThroughput executor and connects to Stampede2's Slurm scheduler.

.. literalinclude:: ../../parsl/configs/stampede2.py


Summit (ORNL)
-------------

.. image:: https://www.olcf.ornl.gov/wp-content/uploads/2018/06/Summit_Exaop-1500x844.jpg

The following snippet shows an example configuration for executing from the login node on Summit, the leadership class supercomputer hosted at the Oak Ridge National Laboratory.
The example uses the `parsl.providers.LSFProvider` to provision compute nodes from the LSF cluster scheduler and the `parsl.launchers.JsrunLauncher` to launch workers across the compute nodes.

.. literalinclude:: ../../parsl/configs/summit.py


TOSS3 (LLNL)
------------

.. image:: https://hpc.llnl.gov/sites/default/files/Magma--2020-LLNL.jpg

The following snippet shows an example configuration for executing on one of LLNL's **TOSS3**
machines, such as Quartz, Ruby, Topaz, Jade, or Magma. This example uses the `parsl.executors.FluxExecutor`
and connects to Slurm using the `parsl.providers.SlurmProvider`. This configuration assumes that the script
is being executed on the login nodes of one of the machines.

.. literalinclude:: ../../parsl/configs/toss3_llnl.py


Further help
------------

For help constructing a configuration, you can click on class names such as :class:`~parsl.config.Config` or :class:`~parsl.executors.HighThroughputExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the python command line via, for example:

.. code-block:: python

    from parsl.config import Config
    help(Config)
