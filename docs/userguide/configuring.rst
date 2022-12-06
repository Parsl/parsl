.. _configuration-section:

Configuration
=============

Parsl separates program logic from execution configuration, enabling
programs to be developed entirely independently from their execution
environment. Configuration is described by a Python object (:class:`~parsl.config.Config`) 
so that developers can 
introspect permissible options, validate settings, and retrieve/edit
configurations dynamically during execution. A configuration object specifies 
details of the provider, executors, connection channel, allocation size, 
queues, durations, and data management options. 

The following example shows a basic configuration object (:class:`~parsl.config.Config`) for the Frontera
supercomputer at TACC.
This config uses the `parsl.executors.HighThroughputExecutor` to submit
tasks from a login node (`parsl.channels.LocalChannel`). It requests an allocation of
128 nodes, deploying 1 worker for each of the 56 cores per node, from the normal partition.
The config uses the `address_by_hostname()` helper function to determine
the login node's IP address.

.. code-block:: python

    from parsl.config import Config
    from parsl.channels import LocalChannel
    from parsl.providers import SlurmProvider
    from parsl.executors import HighThroughputExecutor
    from parsl.launchers import SrunLauncher
    from parsl.addresses import address_by_hostname

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="frontera_htex",
                address=address_by_hostname(),
                max_workers=56,
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

.. note::
   All configuration examples below must be customized for the user's 
   allocation, Python environment, file system, etc.

How to Configure
----------------

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
|                     | * `parsl.executors.WorkQueueExecutor` beta_   |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Amazon Web Services | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.AWSProvider`          |
+---------------------+-----------------------------------------------+----------------------------------------+
| Google Cloud        | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.GoogleCloudProvider`  |
+---------------------+-----------------------------------------------+----------------------------------------+
| Slurm based system  | * `parsl.executors.ExtremeScaleExecutor`      | `parsl.providers.SlurmProvider`        |
|                     | * `parsl.executors.HighThroughputExecutor`    |                                        |
|                     | * `parsl.executors.WorkQueueExecutor` beta_   |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Torque/PBS based    | * `parsl.executors.ExtremeScaleExecutor`      | `parsl.providers.TorqueProvider`       |
| system              | * `parsl.executors.HighThroughputExecutor`    |                                        |
|                     | * `parsl.executors.WorkQueueExecutor` beta_   |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Cobalt based system | * `parsl.executors.ExtremeScaleExecutor`      | `parsl.providers.CobaltProvider`       |
|                     | * `parsl.executors.HighThroughputExecutor`    |                                        |
|                     | * `parsl.executors.WorkQueueExecutor` beta_   |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| GridEngine based    | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.GridEngineProvider`   |
| system              | * `parsl.executors.WorkQueueExecutor` beta_   |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Condor based        | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.CondorProvider`       |
| cluster or grid     | * `parsl.executors.WorkQueueExecutor` beta_   |                                        |
+---------------------+-----------------------------------------------+----------------------------------------+
| Kubernetes cluster  | * `parsl.executors.HighThroughputExecutor`    | `parsl.providers.KubernetesProvider`   |
+---------------------+-----------------------------------------------+----------------------------------------+

.. _beta:

WorkQueueExecutor is available in ``v1.0.0`` in beta status.


2.  How many nodes will be used to execute the apps? What task durations are necessary to achieve good performance?


+------------------------------------------+----------------------+-------------------------------------+
| Executor                                 | Number of Nodes [*]_ | Task duration for good performance  |
+==========================================+======================+=====================================+
| `parsl.executors.ThreadPoolExecutor`     | 1 (Only local)       | Any                                 |
+------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.HighThroughputExecutor` | <=2000               | Task duration(s)/#nodes >= 0.01     |
|                                          |                      | longer tasks needed at higher scale |
+------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.ExtremeScaleExecutor`   | >1000, <=8000 [*]_   | >minutes                            |
+------------------------------------------+----------------------+-------------------------------------+
| `parsl.executors.WorkQueueExecutor`      | <=1000 [*]_          | 10s+                                |
+------------------------------------------+----------------------+-------------------------------------+


.. [*] Assuming 32 workers per node. If there are fewer workers launched
       per node, a larger number of nodes could be supported.

.. [*] 8,000 nodes with 32 workers (256,000 workers) is the maximum scale at which
       the `parsl.executors.ExtremeScaleExecutor` has been tested.

.. [*] The maximum number of nodes tested for the `parsl.executors.WorkQueueExecutor` is 10,000 GPU cores and
       20,000 CPU cores.


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
| `parsl.providers.CobaltProvider`    | Any                      | * `parsl.launchers.AprunLauncher`                  |
+-------------------------------------+--------------------------+----------------------------------------------------+
| `parsl.providers.SlurmProvider`     | Any                      | * `parsl.launchers.SrunLauncher`  if native slurm  |
|                                     |                          | * `parsl.launchers.AprunLauncher`, otherwise       |
+-------------------------------------+--------------------------+----------------------------------------------------+

.. note:: If using a Cray system, you most likely need to use the `parsl.launchers.AprunLauncher` to launch workers unless you
          are on a **native Slurm** system like :ref:`configuring_nersc_cori`


4) Where will the main Parsl program run and how will it communicate with the apps?

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

Many modern clusters provide multiple accelerators per compute note, yet many applications are best suited to using a single accelerator per task.
Parsl supports pinning each worker to difference accelerators using ``available_accelerators`` option of the :class:`~parsl.executors.HighThroughputExecutor`.
Provide either the number of executors (Parsl will assume they are named in integers starting from zero) or a list of the names of the accelerators available on the node.

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


Multi-Threaded Applications
---------------------------

Workflows which launch multiple workers on a single node which perform multi-threaded tasks (e.g., NumPy, Tensorflow operations) may run into thread contention issues.
Each worker may try to use the same hardware threads, which leads to performance penalties.
Use the ``cpu_affinity`` feature of the :class:`~parsl.executors.HighThroughputExecutor` to assign workers to specific cores.
Parsl provides a 'block' or 'alternate' option on how cores are pinned to each worker (ex: 4 cores are grouped (0, 1) and (2, 3) for block,
(0, 2) and (1, 3) for alternate).
Select the best blocking strategy for processor's cache hierarchy (choose 'alternate' if in doubt) to ensure workers to not compete for cores.

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                cpu_affinity='alternate',
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

Ad-Hoc Clusters
---------------

Any collection of compute nodes without a scheduler can be considered an
ad-hoc cluster. Often these machines have a shared file system such as NFS or Lustre.
In order to use these resources with Parsl, they need to set-up for password-less SSH access.

To use these ssh-accessible collection of nodes as an ad-hoc cluster, we use
the `parsl.providers.AdHocProvider` with an `parsl.channels.SSHChannel` to each node. An example
configuration follows.

.. literalinclude:: ../../parsl/configs/ad_hoc.py

.. note::
   Multiple blocks should not be assigned to each node when using the `parsl.executors.HighThroughputExecutor`

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


Blue Waters (NCSA)
------------------

.. image:: https://www.cray.com/sites/default/files/images/Solutions_Images/bluewaters.png

The following snippet shows an example configuration for executing remotely on Blue Waters, a flagship machine at the National Center for Supercomputing Applications.
The configuration assumes the user is running on a login node and uses the `parsl.providers.TorqueProvider` to interface
with the scheduler, and uses the `parsl.launchers.AprunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/bluewaters.py

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
The configuration uses the `parsl.providers.LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support yet.
This system uses Grid Engine which Parsl interfaces with using the `parsl.providers.GridEngineProvider`.

.. literalinclude:: ../../parsl/configs/cc_in2p3.py


CCL (Notre Dame, with Work Queue)
---------------------------------

.. image:: http://ccl.cse.nd.edu/software/workqueue/WorkQueueLogoSmall.png

To utilize Work Queue with Parsl, please install the full CCTools software package within an appropriate Anaconda or Miniconda environment
(instructions for installing Miniconda can be found `in the Conda install guide <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_):

.. code-block:: bash

   $ conda create -y --name <environment> python=<version> conda-pack
   $ conda activate <environment>
   $ conda install -y -c conda-forge ndcctools parsl

This creates a Conda environment on your machine with all the necessary tools and setup needed to utilize Work Queue with the Parsl library.

The following snippet shows an example configuration for using the Work Queue distributed framework to run applications on remote machines at large.
This examples uses the `parsl.executors.WorkQueueExecutor` to schedule tasks locally,
and assumes that Work Queue workers have been externally connected to the master using the
`work_queue_factory <https://cctools.readthedocs.io/en/latest/man_pages/work_queue_factory/>`_ or
`condor_submit_workers <https://cctools.readthedocs.io/en/latest/man_pages/condor_submit_workers/>`_ command line utilities from CCTools.
For more information on using Work Queue or to get help with running applications using CCTools,
visit the `CCTools documentation online <https://cctools.readthedocs.io/en/latest/help/>`_.

.. literalinclude::  ../../parsl/configs/wqex_local.py

Comet (SDSC)
------------

.. image:: https://ucsdnews.ucsd.edu/news_uploads/comet-logo.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer
Center's **Comet** supercomputer. The example is designed to be executed on the login nodes, using the
`parsl.providers.SlurmProvider` to interface with the Slurm scheduler used by Comet and the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/comet.py


Cooley (ALCF)
-------------

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's
**Cooley** analysis and visualization system.
The example uses the `parsl.executors.HighThroughputExecutor` and connects to Cooley's Cobalt scheduler
using the `parsl.providers.CobaltProvider`. This configuration assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/cooley.py


.. _configuring_nersc_cori:

Cori (NERSC)
------------

.. image:: https://6lli539m39y3hpkelqsm3c2fg-wpengine.netdna-ssl.com/wp-content/uploads/2017/08/Cori-NERSC.png

The following snippet shows an example configuration for accessing NERSC's **Cori** supercomputer. This example uses the `parsl.executors.HighThroughputExecutor` and connects to Cori's Slurm scheduler.
It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. literalinclude:: ../../parsl/configs/cori.py


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

`Polaris <https://www.alcf.anl.gov/support/user-guides/polaris/getting-started/index.html>`_
is a HPE Apollo supercomputer that uses PBSPro to manage 560 nodes each with 4 Nvidia GPUs.
Follow ALCF's guide to `clone the base Anaconda environment <https://www.alcf.anl.gov/support/user-guides/polaris/data-science-workflows/python/index.html#cloning-the-base-anaconda-environment>`_
and install Parsl using pip.
The following configuration places a four workers on each node and pins each to a single GPU.

.. literalinclude:: ../../parsl/configs/polaris.py

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


Theta (ALCF)
------------

.. image:: https://www.alcf.anl.gov/files/ALCF-Theta_111016-1000px.jpg

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's
**Theta** supercomputer. This example uses the `parsl.executors.HighThroughputExecutor` and connects to Theta's Cobalt scheduler
using the `parsl.providers.CobaltProvider`. This configuration assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/theta.py


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
