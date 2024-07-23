.. _configuration-section:

Configuration
=============

Parsl separates program logic from execution configuration, enabling programs to be developed independently of their execution environment. Configuration is described by a Python object (`parsl.config.Config`) so that developers can inspect available options, validate settings, and modify configurations dynamically during execution. A configuration object specifies details about the provider, executors, connection channel, allocation size, queues, durations, and data management options.

The following example shows a basic configuration object (`parsl.config.Config`) for the Frontera supercomputer at TACC. This config uses the `parsl.executors.HighThroughputExecutor` to submit tasks from a login node (`parsl.channels.LocalChannel`). It requests an allocation of 128 nodes, deploying 1 worker for each of the 56 cores per node, from the normal partition. To limit network connections to just the internal network, the config specifies the address used by the infiniband interface with `address_by_interface('ib0')`.

.. code-block:: python

    from parsl.config import Config
    from parsl.channels import LocalChannel
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
                    channel=LocalChannel(),
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

`parsl.config.Config` objects are used to define the "Data Flow Kernel" (DFK) that will manage tasks. All Parsl applications start by creating or importing a configuration and then calling the load function.

.. code-block:: python

    from parsl.configs.htex_local import config
    import parsl

    with parsl.load(config):
        # Your workflow here

The `load` statement can happen after apps are defined but must occur before tasks are started. Loading the Config object within a context manager like `with` is recommended for automatic cleaning of the DFK when exiting the context manager.

The `parsl.config.Config` object cannot be reused after it is loaded. If your application needs to shut down and re-launch the DFK, consider using a configuration function.

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
   All configuration examples below must be customized for the user's allocation, Python environment, file system, etc.

The configuration specifies what resources are to be used for executing the Parsl program and its apps. Consider the following aspects to determine an ideal configuration:
1. Where the Parsl apps will execute.
2. How many nodes will be used to execute the apps, and how long the apps will run.
3. Whether Parsl should request multiple nodes in an individual scheduler job.
4. Where the main Parsl program will run and how it will communicate with the apps.

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
| Cobalt based system | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.CobaltProvider`       |
|                     | * `parsl.executors.WorkQueueExecutor`         |                                        |
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

2. How many nodes will be used to execute the apps? What task durations are necessary to achieve good performance?

+--------------------------------------------+----------------------+-------------------------------------+
| Executor                                   | Number of Nodes [*]_ | Task duration for good performance  |
+============================================+======================+=====================================+
| `parsl.executors.ThreadPoolExecutor`       | 1 (Only local)       | Any                                 |
+--------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.HighThroughputExecutor`   | <=2000               | Task duration(s)/#nodes >= 0.01     |
|                                            |                      | Longer tasks needed at higher scale |
+--------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.WorkQueueExecutor`        | <=1000 [*]_          | 10s+                                |
+--------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.taskvine.TaskVineExecutor`| <=1000 [*]_          | 10s+                                |
+--------------------------------------------+----------------------+-------------------------------------+

.. [*] Assuming 32 workers per node. If there are fewer workers launched per node, a larger number of nodes could be supported.

.. [*] The maximum number of nodes tested for the `parsl.executors.WorkQueueExecutor` is 10,000 GPU cores and 20,000 CPU cores.

.. [*] The maximum number of nodes tested for the `parsl.executors.taskvine.TaskVineExecutor` is 10,000 GPU cores and 20,000 CPU cores.

3. Should Parsl request multiple nodes in an individual scheduler job? (Here the term block is equivalent to a single scheduler job.)

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
| `parsl.providers.CobaltProvider`    | Any                      | * `parsl.launchers.AprunLauncher`                  |
+-------------------------------------+--------------------------+----------------------------------------------------+
| `parsl.providers.SlurmProvider`     | Any                      | * `parsl.launchers.SrunLauncher` if native Slurm   |
|                                     |                          | * `parsl.launchers.AprunLauncher`, otherwise       |
+-------------------------------------+--------------------------+----------------------------------------------------+

.. note::
   If using a Cray system, you most likely need to use the `parsl.launchers.AprunLauncher` to launch workers unless you are on a **native Slurm** system like `NERSC Cori <configuring_nersc_cori>`_.

4. Where will the main Parsl program run and how will it communicate with the apps?

+------------------------+--------------------------+---------------------------------------------------+
| Parsl program location | App execution target     | Suitable channel                                  |
+========================+==========================+===================================================+
| Laptop/Workstation     | Laptop/Workstation       | `parsl.channels.LocalChannel`                     |
+------------------------+--------------------------+---------------------------------------------------+
| Laptop/Workstation     | Cloud Resources          | No channel is needed                              |
+------------------------+--------------------------+---------------------------------------------------+
| Laptop/Workstation     | Clusters with no 2FA     | `parsl.channels.SSHChannel`                       |
+------------------------+--------------------------+---------------------------------------------------+
| Laptop/Workstation     | Clusters with 2FA        | `parsl.channels.SSHInteractiveLoginChannel`       |
+------------------------+--------------------------+---------------------------------------------------+
| Login node             | Cluster/Supercomputer    | `parsl.channels.LocalChannel`                     |
+------------------------+--------------------------+---------------------------------------------------+

Heterogeneous Resources
-----------------------

In some cases, it can be difficult to specify the resource requirements for running a workflow. For example, if the compute nodes a site provides are not uniform, there is no "correct" resource configuration; the amount of parallelism depends on which node (large or small) each job runs on. In addition, the software and filesystem setup can vary from node to node. A Condor cluster may not provide shared filesystem access at all and may include nodes with a variety of Python versions and available libraries.

The `parsl.executors.WorkQueueExecutor` provides several features to work with heterogeneous resources. By default, Parsl only runs one app at a time on each worker node. However, it is possible to specify the requirements for a particular app, and Work Queue will automatically run as many parallel instances as possible on each node. Work Queue automatically detects the number of cores, memory, and other resources available on each execution node. To activate this feature, add a resource specification to your apps. A resource specification is a dictionary with the following three keys: `cores` (an integer corresponding to the number of cores required by the task), `memory` (an integer corresponding to the task's memory requirement in MB), and `disk` (an integer corresponding to the task's disk requirement in MB). The specification can be set for all app invocations via a default, for example:

.. code-block:: python

    @python_app
    def compute(x, parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
        return x * 2

or updated when the app is invoked:

.. code-block:: python

    spec = {'cores': 1, 'memory': 500, 'disk': 500}
    future = compute(x, parsl_resource_specification=spec)

This `parsl_resource_specification` special keyword argument will inform Work Queue about the resources this app requires. When placing instances of `compute(x)`, Work Queue will run as many parallel instances as possible based on each worker node's available resources.

If an app's resource requirements are not known in advance, Work Queue has an auto-labeling feature that measures the actual resource usage of your apps and automatically chooses resource labels for you. With auto-labeling, it is not necessary to provide `parsl_resource_specification`; Work Queue collects stats in the background and updates resource labels as your workflow runs. To activate this feature, add the following flags to your executor config:

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

The `autolabel` flag tells Work Queue to automatically generate resource labels. By default, these labels are shared across all apps in your workflow. The `autocategory` flag puts each app into a different category, so that Work Queue will choose separate resource requirements for each app. This is important if, for example, some of your apps use a single core and some apps require multiple cores. Unless you know that all apps have uniform resource requirements, you should turn on `autocategory` when using `autolabel`.

The Work Queue executor can also help deal with sites that have non-uniform software environments across nodes. Parsl assumes that the Parsl program and the compute nodes all use the same Python version. In addition, any packages your apps import must be available on compute nodes. If no shared filesystem is available or if node configuration varies, this can lead to difficult-to-trace execution problems.

If your Parsl program is running in a Conda environment, the Work Queue executor can automatically scan the imports in your apps, create a self-contained software package, transfer the software package to worker nodes, and run your code inside the packaged and uniform environment. First, make sure that the Conda environment is active and you have the required packages installed (via either `pip` or `conda`):

- `python`
- `parsl`
- `ndcctools`
- `conda-pack`

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
   There will be a noticeable delay the first time Work Queue sees an app; it is creating and packaging a complete Python environment. This packaged environment is cached, so subsequent app invocations should be much faster.

Using this approach, it is possible to run Parsl applications on nodes that don't have Python available at all. The packaged environment includes a Python interpreter, and Work Queue does not require Python to run.

.. note::
   The automatic packaging feature only supports packages installed via `pip` or `conda`. Importing from other locations (e.g. via `$PYTHONPATH`) or importing other modules in the same directory is not supported.

Accelerators
------------

Many modern clusters provide multiple accelerators per compute node, yet many applications are best suited to using a single accelerator per task. Parsl supports pinning each worker to different accelerators using the `available_accelerators` option of the `parsl.executors.HighThroughputExecutor`. Provide either the number of executors (Parsl will assume they are named in integers starting from zero) or a list of the names of the accelerators available on the node.

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                available_accelerators=2,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

For hardware that uses Nvidia devices, Parsl allows for the oversubscription of workers to GPUs. This is intended to make use of Nvidia's `Multi-Process Service (MPS) <https://docs.nvidia.com/deploy/mps/>`_ available on many of their GPUs that allows users to run multiple concurrent processes on a single GPU. The user needs to set in the `worker_init` commands to start MPS on every node in the block (this is machine dependent). The `available_accelerators` option should then be set to the total number of GPU partitions run on a single node in the block. For example, for a node with 4 Nvidia GPUs, to create 8 workers per GPU, set `available_accelerators=32`. GPUs will be assigned to workers in ascending order in contiguous blocks. In the example, workers 0-7 will be placed on GPU 0, workers 8-15 on GPU 1, workers 16-23 on GPU 2, and workers 24-31 on GPU 3.

Multi-Threaded Applications
---------------------------

Workflows that launch multiple workers on a single node which perform multi-threaded tasks (e.g., NumPy, Tensorflow operations) may run into thread contention issues. Each worker may try to use the same hardware threads, which leads to performance penalties. Use the `cpu_affinity` feature of the `parsl.executors.HighThroughputExecutor` to assign workers to specific threads. Users can pin threads to workers either with a strategy method or an explicit list.

The strategy methods will auto-assign all detected hardware threads to workers. Allowed strategies that can be assigned to `cpu_affinity` are `block`, `block-reverse`, and `alternating`. The `block` method pins threads to workers in sequential order (e.g., 4 threads are grouped (0, 1) and (2, 3) on two workers); `block-reverse` pins threads in reverse sequential order (e.g., (3, 2) and (1, 0)); and `alternating` alternates threads among workers (e.g., (0, 2) and (1, 3)).

Select the best blocking strategy for the processor's cache hierarchy (choose `alternating` if in doubt) to ensure workers do not compete for cores.

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                cpu_affinity='alternating',
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

Users can also use `cpu_affinity` to assign threads explicitly to workers with a string that has the format `cpu_affinity="list:<worker1_threads>:<worker2_threads>:<worker3_threads>"`.

Each worker's threads can be specified as a comma-separated list or a hyphenated range: `thread1,thread2,thread3` or `thread_start-thread_end`.

An example for 12 workers on a node with 208 threads is:

.. code-block:: python

    cpu_affinity="list:0-7,104-111:8-15,112-119:16-23,120-127:24-31,128-135:32-39,136-143:40-47,144-151:52-59,156-163:60-67,164-171:68-75,172-179:76-83,180-187:84-91,188-195:92-99,196-203"

This example assigns 16 threads each to 12 workers. Note that in this example there are threads that are skipped. If a thread is not explicitly assigned to a worker, it will be left idle. The number of thread "ranks" (colon-separated thread lists/ranges) must match the total number of workers on the node; otherwise, an exception will be raised.

Thread affinity is accomplished in two ways. Each worker first sets the affinity for the Python process using the `affinity mask <https://docs.python.org/3/library/os.html#os.sched_setaffinity>`, which may not be available on all operating systems. It then sets environment variables to control `OpenMP thread affinity <https://hpc-tutorials.llnl.gov/openmp/ProcessThreadAffinity.pdf>` so that any subprocesses launched by a worker that use OpenMP know which processors are valid. These include `OMP_NUM_THREADS`, `GOMP_COMP_AFFINITY`, and `KMP_THREAD_AFFINITY`.

Ad-Hoc Clusters
---------------

Any collection of compute nodes without a scheduler can be considered an ad-hoc cluster. Often these machines have a shared file system such as NFS or Lustre. To use these resources with Parsl, they need to be set up for password-less SSH access.

To use these SSH-accessible collections of nodes as an ad-hoc cluster, we use the `parsl.providers.AdHocProvider` with a `parsl.channels.SSHChannel` to each node. An example configuration follows.

.. code-block:: python

    from parsl.config import Config
    from parsl.channels import SSHChannel
    from parsl.providers import AdHocProvider
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='htex_adhoc',
                provider=AdHocProvider(
                    worker_init='source activate my_env',
                    nodes=[
                        'node1.myorg.org',
                        'node2.myorg.org'
                    ],
                    channel=SSHChannel(
                        hostname='node1.myorg.org',
                        username='myuser',
                        script_dir='/home/myuser/parsl_scripts'
                    )
                )
            )
        ]
    )

.. note::

   Multiple blocks should not be assigned to each node when using the `parsl.executors.HighThroughputExecutor`.

Amazon Web Services
-------------------

.. image:: ./aws_image.png

.. note::

   To use AWS with Parsl, install Parsl with AWS dependencies via ``python3 -m pip install 'parsl[aws]'``.

Amazon Web Services is a commercial cloud service that allows users to rent a range of computers and other computing services. The following snippet shows how Parsl can be configured to provision nodes from the Elastic Compute Cloud (EC2) service. The first time this configuration is used, Parsl will configure a Virtual Private Cloud and other networking and security infrastructure that will be reused in subsequent executions. The configuration uses the `parsl.providers.AWSProvider` to connect to AWS.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import AWSProvider
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='ec2_htex',
                provider=AWSProvider(
                    image_id='ami-0abcdef1234567890',
                    instance_type='t2.micro',
                    region='us-west-2',
                    key_name='my-key-pair'
                )
            )
        ]
    )

ASPIRE 1 (NSCC)
---------------

.. image:: https://www.nscc.sg/wp-content/uploads/2017/04/ASPIRE1Img.png

The following snippet shows an example configuration for accessing NSCC's **ASPIRE 1** supercomputer. This example uses the `parsl.executors.HighThroughputExecutor` executor and connects to ASPIRE1's PBSPro scheduler. It also shows how the ``scheduler_options`` parameter can be used for scheduling array jobs in PBSPro.

.. code-block:: python

    from parsl.config import Config
    from parsl.channels import SSHChannel
    from parsl.providers import TorqueProvider
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='aspire1_htex',
                provider=TorqueProvider(
                    channel=SSHChannel(
                        hostname='login1.aspire1.nscc.sg',
                        username='myuser',
                        script_dir='/home/myuser/parsl_scripts'
                    ),
                    scheduler_options='#PBS -l select=1:ncpus=24:mem=64gb',
                    worker_init='module load anaconda3',
                    nodes_per_block=1,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1
                )
            )
        ]
    )

Illinois Campus Cluster (UIUC)
------------------------------

.. image:: https://campuscluster.illinois.edu/wp-content/uploads/2018/02/ND2_3633-sm.jpg

The following snippet shows an example configuration for executing on the Illinois Campus Cluster. The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='icc_htex',
                provider=SlurmProvider(
                    nodes_per_block=2,
                    init_blocks=1,
                    partition='batch',
                    account='my_account',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

Bridges (PSC)
-------------

.. image:: https://insidehpc.com/wp-content/uploads/2016/08/Bridges_FB1b.jpg

The following snippet shows an example configuration for executing on the Bridges supercomputer at the Pittsburgh Supercomputing Center. The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='bridges_htex',
                provider=SlurmProvider(
                    nodes_per_block=4,
                    init_blocks=1,
                    partition='RM',
                    account='my_account',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

CC-IN2P3
--------

.. image:: https://cc.in2p3.fr/wp-content/uploads/2017/03/bandeau_accueil.jpg

The snippet below shows an example configuration for executing from a login node on IN2P3's Computing Centre. The configuration uses the `parsl.providers.LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support yet. This system uses Grid Engine which Parsl interfaces with using the `parsl.providers.GridEngineProvider`.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import GridEngineProvider
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='cc_in2p3_htex',
                provider=GridEngineProvider(
                    account='my_account',
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

CCL (Notre Dame, TaskVine)
--------------------------

.. image:: https://ccl.cse.nd.edu/software/taskvine/taskvine-logo.png

To utilize TaskVine with Parsl, please install the full CCTools software package within an appropriate Anaconda or Miniconda environment (instructions for installing Miniconda can be found in the Conda install guide):

.. code-block:: bash

    $ conda create -y --name <environment> python=<version> conda-pack
    $ conda activate <environment>
    $ conda install -y -c conda-forge ndcctools parsl

This creates a Conda environment on your machine with all the necessary tools and setup needed to utilize TaskVine with the Parsl library.

The following snippet shows an example configuration for using the Parsl/TaskVine executor to run applications on the local machine. This example uses the `parsl.executors.taskvine.TaskVineExecutor` to schedule tasks, and a local worker will be started automatically. For more information on using TaskVine, including configurations for remote execution, visit the `TaskVine/Parsl documentation online <https://cctools.readthedocs.io/en/latest/taskvine/#parsl>`_.

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import TaskVineExecutor

    config = Config(
        executors=[
            TaskVineExecutor(
                label='vine_local'
            )
        ]
    )

Expanse (SDSC)
--------------

.. image:: https://www.hpcwire.com/wp-content/uploads/2019/07/SDSC-Expanse-graphic-cropped.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer Center's **Expanse** supercomputer. The example is designed to be executed on the login nodes, using the `parsl.providers.SlurmProvider` to interface with the Slurm scheduler used by Expanse and the `parsl.launchers.SrunLauncher` to launch workers.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='expanse_htex',
                provider=SlurmProvider(
                    nodes_per_block=4,
                    init_blocks=1,
                    partition='compute',
                    account='my_account',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

.. _configuring_nersc_cori:

Perlmutter (NERSC)
------------------

NERSC provides documentation on `how to use Parsl on Perlmutter <https://docs.nersc.gov/jobs/workflow/parsl/>`_.

Frontera (TACC)
---------------

.. image:: https://frontera-portal.tacc.utexas.edu/media/filer_public/2c/fb/2cfbf6ab-818d-42c8-b4d5-9b39eb9d0a05/frontera-banner-home.jpg

Deployed in June 2019, Frontera is the 5th most powerful supercomputer in the world. Frontera replaces the NSF Blue Waters system at NCSA and is the first deployment in the National Science Foundation's petascale computing program. The configuration below assumes that the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='frontera_htex',
                provider=SlurmProvider(
                    nodes_per_block=128,
                    init_blocks=1,
                    partition='normal',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

Kubernetes Clusters
-------------------

.. image:: https://d1.awsstatic.com/PAC/kuberneteslogo.eabc6359f48c8e30b7a138c18177f3fd39338e05.png

Kubernetes is an open-source system for container management, such as automating the deployment and scaling of containers. The snippet below shows an example configuration for deploying pods as workers on a Kubernetes cluster. The `KubernetesProvider` uses the Python Kubernetes API, which assumes that you have kube config in ``~/.kube/config``.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import KubernetesProvider
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='k8s_htex',
                provider=KubernetesProvider(
                    namespace='default',
                    image='my_docker_image'
                )
            )
        ]
    )

Midway (RCC, UChicago)
----------------------

.. image:: https://rcc.uchicago.edu/sites/rcc.uchicago.edu/files/styles/slideshow-image/public/uploads/images/slideshows/20140430_RCC_8978.jpg?itok=BmRuJ-wq

The Midway cluster is a campus cluster hosted by the Research Computing Center at the University of Chicago. The snippet below shows an example configuration for executing remotely on Midway. The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='midway_htex',
                provider=SlurmProvider(
                    nodes_per_block=4,
                    init_blocks=1,
                    partition='broadwl',
                    account='my_account',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

Open Science Grid
-----------------

.. image:: https://www.renci.org/wp-content/uploads/2008/10/osg_logo.png

The Open Science Grid (OSG) is a national, distributed computing Grid spanning over 100 individual sites to provide tens of thousands of CPU cores. The snippet below shows an example configuration for executing remotely on OSG. You will need to have a valid project name on the OSG. The configuration uses the `parsl.providers.CondorProvider` to interface with the scheduler.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import CondorProvider
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='osg_htex',
                provider=CondorProvider(
                    project='my_project',
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

Polaris (ALCF)
--------------

.. image:: https://www.alcf.anl.gov/sites/default/files/styles/965x543/public/2022-07/33181D_086_ALCF%20Polaris%20Crop.jpg?itok=HVAHsZtt

ALCF provides documentation on `how to use Parsl on Polaris <https://docs.alcf.anl.gov/polaris/workflows/parsl/>`_.

Stampede2 (TACC)
----------------

.. image:: https://www.tacc.utexas.edu/documents/1084364/1413880/stampede2-0717.jpg/

The following snippet shows an example configuration for accessing TACC's **Stampede2** supercomputer. This example uses the `parsl.executors.HighThroughputExecutor` and connects to Stampede2's Slurm scheduler.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='stampede2_htex',
                provider=SlurmProvider(
                    nodes_per_block=4,
                    init_blocks=1,
                    partition='normal',
                    account='my_account',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

Summit (ORNL)
-------------

.. image:: https://www.olcf.ornl.gov/wp-content/uploads/2018/06/Summit_Exaop-1500x844.jpg

The following snippet shows an example configuration for executing from the login node on Summit, the leadership-class supercomputer hosted at the Oak Ridge National Laboratory. The example uses the `parsl.providers.LSFProvider` to provision compute nodes from the LSF cluster scheduler and the `parsl.launchers.JsrunLauncher` to launch workers across the compute nodes.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import LSFProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import JsrunLauncher

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='summit_htex',
                provider=LSFProvider(
                    nodes_per_block=4,
                    init_blocks=1,
                    account='my_account',
                    launcher=JsrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

TOSS3 (LLNL)
------------

.. image:: https://hpc.llnl.gov/sites/default/files/Magma--2020-LLNL.jpg

The following snippet shows an example configuration for executing on one of LLNL's **TOSS3** machines, such as Quartz, Ruby, Topaz, Jade, or Magma. This example uses the `parsl.executors.FluxExecutor` and connects to Slurm using the `parsl.providers.SlurmProvider`. This configuration assumes that the script is being executed on the login nodes of one of the machines.

.. code-block:: python

    from parsl.config import Config
    from parsl.providers import SlurmProvider
    from parsl.executors import FluxExecutor
    from parsl.launchers import SrunLauncher

    config = Config(
        executors=[
            FluxExecutor(
                label='toss3_flux',
                provider=SlurmProvider(
                    nodes_per_block=4,
                    init_blocks=1,
                    partition='normal',
                    account='my_account',
                    launcher=SrunLauncher(),
                    worker_init='module load anaconda3'
                )
            )
        ]
    )

Further Help
------------

For help constructing a configuration, you can click on class names such as `parsl.config.Config` or `parsl.executors.HighThroughputExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the Python command line via, for example:

.. code-block:: python

    from parsl.config import Config
    help(Config)
