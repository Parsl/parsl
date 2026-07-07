Example configurations
======================

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

Ad-Hoc Clusters
---------------

Parsl's support of ad-hoc clusters of compute nodes without a scheduler
is deprecated.

See
`issue #3515 <https://github.com/Parsl/parsl/issues/3515>`_
for further discussion.

Amazon Web Services
-------------------

.. image:: img/aws_image.png

.. note::
   To use AWS with Parsl, install Parsl with AWS dependencies via ``python3 -m pip install 'parsl[aws]'``

Amazon Web Services is a commercial cloud service which allows users to rent a range of computers and other computing services.
The following snippet shows how Parsl can be configured to provision nodes from the Elastic Compute Cloud (EC2) service.
The first time this configuration is used, Parsl will configure a Virtual Private Cloud and other networking and security infrastructure that will be
re-used in subsequent executions. The configuration uses the `parsl.providers.AWSProvider` to connect to AWS.

.. literalinclude:: ../../../parsl/configs/ec2.py

Anvil (Purdue)
--------------

.. image:: https://www.rcac.purdue.edu/storage/resources/anvil/resource.jpg

Anvil is hosted at Purdue University, and is a powerful new supercomputer that provides advanced computing capabilities to support a wide range of computational and data-intensive research spanning from traditional high-performance computing to modern artificial intelligence applications. 
The configuration below provides access to a single GPU on a single node, and assumes that the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude::  ../../../parsl/configs/anvil.py

ASPIRE 1 (NSCC)
---------------

.. image:: https://www.nscc.sg/wp-content/uploads/2017/04/ASPIRE1Img.png

The following snippet shows an example configuration for accessing NSCC's **ASPIRE 1** supercomputer. This example uses the `parsl.executors.HighThroughputExecutor` executor and connects to ASPIRE1's PBSPro scheduler. It also shows how ``scheduler_options`` parameter could be used for scheduling array jobs in PBSPro.

.. literalinclude:: ../../../parsl/configs/ASPIRE1.py




Illinois Campus Cluster (UIUC)
------------------------------

.. image:: https://campuscluster.illinois.edu/wp-content/uploads/2018/02/ND2_3633-sm.jpg

The following snippet shows an example configuration for executing on the Illinois Campus Cluster.
The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface
with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../../parsl/configs/illinoiscluster.py

Bridges (PSC)
-------------

.. image:: https://insidehpc.com/wp-content/uploads/2016/08/Bridges_FB1b.jpg

The following snippet shows an example configuration for executing on the Bridges supercomputer at the Pittsburgh Supercomputing Center.
The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface
with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../../parsl/configs/bridges.py



CC-IN2P3
--------

.. image:: https://cc.in2p3.fr/wp-content/uploads/2017/03/bandeau_accueil.jpg

The snippet below shows an example configuration for executing from a login node on IN2P3's Computing Centre.
The configuration uses the `parsl.providers.LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support.
This system uses Grid Engine which Parsl interfaces with using the `parsl.providers.GridEngineProvider`.

.. literalinclude:: ../../../parsl/configs/cc_in2p3.py


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

.. literalinclude::  ../../../parsl/configs/vineex_local.py

TaskVine's predecessor, WorkQueue, may continue to be used with Parsl.
For more information on using WorkQueue visit the `CCTools documentation online <https://cctools.readthedocs.io/en/latest/help/>`_.

Delta (NCSA)
--------------

.. image:: https://docs.ncsa.illinois.edu/systems/delta/en/latest/_images/delta_front_1.png

Delta is a dedicated, ACCESS-allocated resource designed by HPE and NCSA, delivering a highly capable GPU-focused compute environment for GPU and CPU workloads.
The configuration below provides access to a single GPU on a single node, and assumes that the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude::  ../../../parsl/configs/delta.py

Expanse (SDSC)
--------------

.. image:: https://www.hpcwire.com/wp-content/uploads/2019/07/SDSC-Expanse-graphic-cropped.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer
Center's **Expanse** supercomputer. The example is designed to be executed on the login nodes, using the
`parsl.providers.SlurmProvider` to interface with the Slurm scheduler used by Comet and the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../../parsl/configs/expanse.py


Improv (Argonne LCRC)
---------------------

.. image:: https://www.lcrc.anl.gov/sites/default/files/styles/965_wide/public/2023-12/20231214_114057.jpg?itok=A-Rz5pP9

**Improv** is a PBS Pro based  supercomputer at Argonne's Laboratory Computing Resource
Center (LCRC). The following snippet is an example configuration that uses `parsl.providers.PBSProProvider`
and `parsl.launchers.MpiRunLauncher` to run on multinode jobs.

.. literalinclude:: ../../../parsl/configs/improv.py


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

.. literalinclude:: ../../../parsl/configs/frontera.py

Globus Compute (Multisite)
--------------------------

Globus Compute is a distributed Function as a Service (FaaS) platform that enables secure
execution of functions on heterogeneous remote computers, from laptops to campus clusters, clouds, and supercomputers.
Functions are executed on `Globus Compute Endpoints <https://globus-compute.readthedocs.io/en/latest/endpoints/endpoints.html>`_
that can be `configured <https://globus-compute.readthedocs.io/en/latest/endpoints/endpoint_examples.html>`_
for most clusters/HPC systems. The example configuration below allows task submission
to Globus Compute's hosted tutorial endpoint.

.. literalinclude:: ../../../parsl/configs/gc_tutorial.py

.. note:: The Globus Compute tutorial endpoint runs Python 3.11. We recommend
   using the same Python environment to avoid potential serialization errors
   caused by environment mismatches. Globus Compute will raise a warning if any
   environment version mismatches are detected although minor version differences
   may not cause faults (eg, Python3.11.7 vs Python3.11.8)

The configuration below specifies two remote endpoints, one at `SDSC's Expanse Supercomputer <https://www.sdsc.edu/services/hpc/expanse/>`_
and the other at `NERSC's Perlmutter Supercomputer <https://docs.nersc.gov/systems/perlmutter/architecture/>`_.

.. literalinclude:: ../../../parsl/configs/gc_multisite.py



Kubernetes Clusters
-------------------

.. image:: https://d1.awsstatic.com/PAC/kuberneteslogo.eabc6359f48c8e30b7a138c18177f3fd39338e05.png

Kubernetes is an open-source system for container management, such as automating deployment and scaling of containers.
The snippet below shows an example configuration for deploying pods as workers on a Kubernetes cluster.
The KubernetesProvider exploits the Python Kubernetes API, which assumes that you have kube config in ``~/.kube/config``.

.. literalinclude:: ../../../parsl/configs/kubernetes.py


Midway (RCC, UChicago)
----------------------

.. image:: https://rcc.uchicago.edu/sites/rcc.uchicago.edu/files/styles/slideshow-image/public/uploads/images/slideshows/20140430_RCC_8978.jpg?itok=BmRuJ-wq

This Midway cluster is a campus cluster hosted by the Research Computing Center at the University of Chicago.
The snippet below shows an example configuration for executing remotely on Midway.
The configuration assumes the user is running on a login node and uses the `parsl.providers.SlurmProvider` to interface
with the scheduler, and uses the `parsl.launchers.SrunLauncher` to launch workers.

.. literalinclude:: ../../../parsl/configs/midway.py


Open Science Grid
-----------------

.. image:: https://www.renci.org/wp-content/uploads/2008/10/osg_logo.png

The Open Science Grid (OSG) is a national, distributed computing Grid spanning over 100 individual sites to provide tens of thousands of CPU cores.
The snippet below shows an example configuration for executing remotely on OSG. You will need to have a valid project name on the OSG.
The configuration uses the `parsl.providers.CondorProvider` to interface with the scheduler.

.. literalinclude:: ../../../parsl/configs/osg.py


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

.. literalinclude:: ../../../parsl/configs/stampede2.py


Summit (ORNL)
-------------

.. image:: https://www.olcf.ornl.gov/wp-content/uploads/2018/06/Summit_Exaop-1500x844.jpg

The following snippet shows an example configuration for executing from the login node on Summit, the leadership class supercomputer hosted at the Oak Ridge National Laboratory.
The example uses the :class:`parsl.providers.LSFProvider` to provision compute nodes from the LSF cluster scheduler and the `parsl.launchers.JsrunLauncher` to launch workers across the compute nodes.

.. literalinclude:: ../../../parsl/configs/summit.py


TOSS3 (LLNL)
------------

.. image:: https://hpc.llnl.gov/sites/default/files/Magma--2020-LLNL.jpg

The following snippet shows an example configuration for executing on one of LLNL's **TOSS3**
machines, such as Quartz, Ruby, Topaz, Jade, or Magma. This example uses the `parsl.executors.FluxExecutor`
and connects to Slurm using the `parsl.providers.SlurmProvider`. This configuration assumes that the script
is being executed on the login nodes of one of the machines.

.. literalinclude:: ../../../parsl/configs/toss3_llnl.py
