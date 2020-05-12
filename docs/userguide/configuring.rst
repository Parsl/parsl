.. _configuration-section:

Configuration
=============

Parsl workflows are developed completely independently from their execution environment.
There are very many different execution environments in which Parsl programs and their apps can run, and
many of these environments have multiple options of how those Parsl programs and apps run, which makes
configuration somewhat complex, and also makes determining how to set up Parsl's configuration
for a particular set of choices fairly complex, though we think the actual configuration
itself is reasonable simple.

Parsl offers an extensible configuration model through which the execution environment and
communication within that environment is configured. Parsl is configured
using :class:`~parsl.config.Config` object. For more information, see
the :class:`~parsl.config.Config` class documentation. The following shows how the
configuration can be specified.

   .. code-block:: python

      import parsl
      from parsl.config import Config
      from parsl.executors.threads import ThreadPoolExecutor

      config = Config(
          executors=[ThreadPoolExecutor()],
          lazy_errors=True
      )
      parsl.load(config)

.. contents:: Configuration How-To and Examples:

.. note::
   Please note that all configuration examples below require customization for your account, 
   allocation, Python environment, etc. 

How to Configure
----------------

The configuration provided to Parsl tells Parsl what resources to use to run the Parsl
program and apps, and how to use them.
Therefore it is important to carefully evaluate certain aspects of the Parsl program and apps,
and the planned compute resources, to determine an ideal configuration match. These aspects are:
1) where the Parsl apps will execute;
2) how many nodes will be used to execute the apps, and how long the apps will run;
3) should the scheduler allocate multiple nodes at one time; and
4) where will the main parsl program run and how will it communicate with the apps.

Stepping through the following question should help you formulate a suitable configuration.
In addition, examples for some specific configurations follow.


1. Where would you like the apps in the Parsl program to run?

+---------------------+----------------------------+------------------------+
| Target              | Executor                   | Provider               |
+=====================+============================+========================+
| Laptop/Workstation  | * `ThreadPoolExecutor`     | `LocalProvider`        |
|                     | * `IPyParallelExecutor`    |                        |
|                     | * `HighThroughputExecutor` |                        |
|                     | * `ExtremeScaleExecutor`   |                        |
+---------------------+----------------------------+------------------------+
| Amazon Web Services | * `IPyParallelExecutor`    | `AWSProvider`          |
|                     | * `HighThroughputExecutor` |                        |
+---------------------+----------------------------+------------------------+
| Google Cloud        | * `IPyParallelExecutor`    | `GoogleCloudProvider`  |
|                     | * `HighThroughputExecutor` |                        |
+---------------------+----------------------------+------------------------+
| Slurm based cluster | * `IPyParallelExecutor`    | `SlurmProvider`        |
| or supercomputer    | * `HighThroughputExecutor` |                        |
|                     | * `ExtremeScaleExecutor`   |                        |
+---------------------+----------------------------+------------------------+
| Torque/PBS based    | * `IPyParallelExecutor`    | `TorqueProvider`       |
| cluster or          | * `HighThroughputExecutor` |                        |
| supercomputer       | * `ExtremeScaleExecutor`   |                        |
+---------------------+----------------------------+------------------------+
| Cobalt based cluster| * `IPyParallelExecutor`    | `CobaltProvider`       |
| or supercomputer    | * `HighThroughputExecutor` |                        |
|                     | * `ExtremeScaleExecutor`   |                        |
+---------------------+----------------------------+------------------------+
| GridEngine based    | * `IPyParallelExecutor`    | `GridEngineProvider`   |
| cluster or grid     | * `HighThroughputExecutor` |                        |
+---------------------+----------------------------+------------------------+
| Condor based        | * `IPyParallelExecutor`    | `CondorProvider`       |
| cluster or grid     | * `HighThroughputExecutor` |                        |
+---------------------+----------------------------+------------------------+
| Kubernetes cluster  | * `IPyParallelExecutor`    | `KubernetesProvider`   |
|                     | * `HighThroughputExecutor` |                        |
+---------------------+----------------------------+------------------------+

2. How many nodes will you use to run them? What task durations give good performance on different executors?


+--------------------------+----------------------+------------------------------------+
| Executor                 | Number of Nodes [*]_ | Task duration for good performance |
+==========================+======================+====================================+
| `ThreadPoolExecutor`     | 1 (Only local)       |  Any                               |
+--------------------------+----------------------+------------------------------------+
| `LowLatencyExecutor`     | <=10                 |  10ms+                             |
+--------------------------+----------------------+------------------------------------+
| `IPyParallelExecutor`    | <=128                |  50ms+                             |
+--------------------------+----------------------+------------------------------------+
| `HighThroughputExecutor` | <=2000               |  Task duration(s)/#nodes >= 0.01   |
|                          |                      | longer tasks needed at higher scale|
+--------------------------+----------------------+------------------------------------+
| `ExtremeScaleExecutor`   | >1000, <=8000 [*]_   |  >minutes                          |
+--------------------------+----------------------+------------------------------------+
| `WorkQueueExecutor`      | <=20000 [*]_         |  10s+                              |
+--------------------------+----------------------+------------------------------------+


.. [*] We assume that each node has 32 workers. If there are fewer workers launched
       per node, a higher number of nodes could be supported.

.. [*] 8000 nodes with 32 workers each totalling 256000 workers is the maximum scale at which
       we've tested the `ExtremeScaleExecutor`.

.. [*] The maximum number of nodes tested for the `WorkQueueExecutor` is 10000 GPU cores and 
       20000 CPU cores.

.. warning:: `IPyParallelExecutor` will be deprecated as of Parsl v0.8.0, with `HighThroughputExecutor`
             as the recommended replacement.


3. If you are running on a cluster or supercomputer, will you request multiple nodes per batch (scheduler) job?
(Here we use the term block to be equivalent to a batch job.)

+----------------------------------------------------------------------------+
| ``nodes_per_block = 1``                                                    |
+---------------------+--------------------------+---------------------------+
| Provider            | Executor choice          | Suitable Launchers        |
+=====================+==========================+===========================+
| Systems that don't  | Any                      | * `SingleNodeLauncher`    |
| use Aprun           |                          | * `SimpleLauncher`        |
+---------------------+--------------------------+---------------------------+
| Aprun based systems | Any                      | * `AprunLauncher`         |
|                     |                          |                           |
+---------------------+--------------------------+---------------------------+

+-------------------------------------------------------------------------------------+
| ``nodes_per_block > 1``                                                             |
+---------------------+--------------------------+------------------------------------+
| Provider            | Executor choice          | Suitable Launchers                 |
+=====================+==========================+====================================+
| `TorqueProvider`    | Any                      | * `AprunLauncher`                  |
|                     |                          | * `MpiExecLauncher`                |
+---------------------+--------------------------+------------------------------------+
| `CobaltProvider`    | Any                      | * `AprunLauncher`                  |
+---------------------+--------------------------+------------------------------------+
| `SlurmProvider`     | Any                      | * `SrunLauncher`  if native slurm  |
|                     |                          | * `AprunLauncher`, otherwise       |
+---------------------+--------------------------+------------------------------------+

.. note:: If you are on a Cray system, you most likely need the `AprunLauncher` to launch workers unless you
          are on a **native Slurm** system like :ref:`configuring_nersc_cori`


4. Where will you run the main Parsl program, given that you already have determined where the apps will run?
(This is needed to determine how to communicate between the Parsl program and the apps.)

+------------------------+--------------------------+------------------------------------+
| Parsl program location | App execution target     | Suitable channel                   |
+========================+==========================+====================================+
| Laptop/Workstation     | Laptop/Workstation       | `LocalChannel`                     |
+------------------------+--------------------------+------------------------------------+
| Laptop/Workstation     | Cloud Resources          | None                               |
+------------------------+--------------------------+------------------------------------+
| Laptop/Workstation     | Clusters with no 2FA     | `SSHChannel`                       |
+------------------------+--------------------------+------------------------------------+
| Laptop/Workstation     | Clusters with 2FA        | `SSHInteractiveLoginChannel`       |
+------------------------+--------------------------+------------------------------------+
| Login node             | Cluster/Supercomputer    | `LocalChannel`                     |
+------------------------+--------------------------+------------------------------------+


Comet (SDSC)
------------

.. image:: https://ucsdnews.ucsd.edu/news_uploads/comet-logo.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer
Center's **Comet** supercomputer. The example is designed to be executed on the login nodes, using the
`SlurmProvider` to interface with the Slurm scheduler used by Comet and the `SrunLauncher` to launch workers.

.. warning:: This config has **NOT** been tested with Parsl v0.9.0

.. literalinclude:: ../../parsl/configs/comet.py


.. _configuring_nersc_cori:

Cori (NERSC)
------------

.. image:: https://6lli539m39y3hpkelqsm3c2fg-wpengine.netdna-ssl.com/wp-content/uploads/2017/08/Cori-NERSC.png

The following snippet shows an example configuration for accessing NERSC's **Cori** supercomputer. This example uses the `HighThroughputExecutor` and connects to Cori's Slurm scheduler.
It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. literalinclude:: ../../parsl/configs/cori.py


Stampede2 (TACC)
------------

.. image:: https://www.tacc.utexas.edu/documents/1084364/1413880/stampede2-0717.jpg/

The following snippet shows an example configuration for accessing TACC's **Stampede2** supercomputer. This example uses theHighThroughput executor and connects to Stampede2's Slurm scheduler. 

.. literalinclude:: ../../parsl/configs/stampede2.py


ASPIRE 1 (NSCC)
------------

.. image:: https://www.nscc.sg/wp-content/uploads/2017/04/ASPIRE1Img.png

The following snippet shows an example configuration for accessing NSCC's **ASPIRE 1** supercomputer. This example uses the `HighThroughputExecutor` executor and connects to ASPIRE1's PBSPro scheduler. It also shows how `scheduler_options` parameter could be used for scheduling array jobs in PBSPro.

.. literalinclude:: ../../parsl/configs/ASPIRE1.py


Frontera (TACC)
---------------

.. image:: https://fronteraweb.tacc.utexas.edu/media/filer_public/e2/66/e266466f-502e-4bfe-92d6-3634d697ed99/frontera-home.jpg

Deployed in June 2019, Frontera is the 5th most powerful supercomputer in the world. Frontera replaces the NSF Blue Waters system at NCSA
and is the first deployment in the National Science Foundation's petascale computing program. The configuration below assumes that the user is
running on a login node and uses the `SlurmProvider` to interface with the scheduler, and uses the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/frontera.py


Theta (ALCF)
------------

.. image:: https://www.alcf.anl.gov/files/ALCF-Theta_111016-1000px.jpg

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's
**Theta** supercomputer. This example uses the `HighThroughputExecutor` and connects to Theta's Cobalt scheduler
using the `CobaltProvider`. This configuration assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/theta.py


Cooley (ALCF)
-------------

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's 
**Cooley** analysis and visualization system.
The example uses the `HighThroughputExecutor` and connects to Cooley's Cobalt scheduler 
using the `CobaltProvider`. This configuration assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/cooley.py


Blue Waters (NCSA)
-------------

.. image:: https://www.cray.com/sites/default/files/images/Solutions_Images/bluewaters.png

The following snippet shows an example configuration for executing remotely on Blue Waters, a flagship machine at the National Center for Supercomputing Applications.
The configuration assumes the user is running on a login node and uses the `TorqueProvider` to interface
with the scheduler, and uses the `AprunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/bluewaters.py


Summit (ORNL)
-------------

.. image:: https://www.olcf.ornl.gov/wp-content/uploads/2018/06/Summit_Exaop-1500x844.jpg

The following snippet shows an example configuration for executing from the login node on Summit, the leadership class supercomputer hosted at the Oak Ridge National Laboratory.
The example uses the `LSFProvider` to provision compute nodes from the LSF cluster scheduler and the `JsrunLauncher` to launch workers across the compute nodes.

.. literalinclude:: ../../parsl/configs/summit.py


CC-IN2P3
--------

.. image:: https://cc.in2p3.fr/wp-content/uploads/2017/03/bandeau_accueil.jpg

The snippet below shows an example configuration for executing from a login node on IN2P3's Computing Centre.
The configuration uses the `LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support yet.
This system uses Grid Engine which Parsl interfaces with using the `GridEngineProvider`.

.. literalinclude:: ../../parsl/configs/cc_in2p3.py

Midway (RCC, UChicago)
----------------------

.. image:: https://rcc.uchicago.edu/sites/rcc.uchicago.edu/files/styles/slideshow-image/public/uploads/images/slideshows/20140430_RCC_8978.jpg?itok=BmRuJ-wq

This Midway cluster is a campus cluster hosted by the Research Computing Center at the University of Chicago.
The snippet below shows an example configuration for executing remotely on Midway.
The configuration assumes the user is running on a login node and uses the `SlurmProvider` to interface
with the scheduler, and uses the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/midway.py


Open Science Grid
-----------------

.. image:: https://hcc-docs.unl.edu/download/attachments/11635314/Screen%20Shot%202013-03-19%20at%202.19.28%20PM.png?version=1&modificationDate=1492720049000&api=v2

The Open Science Grid (OSG) is a national, distributed computing Grid spanning over 100 individual sites to provide tens of thousands of CPU cores.
The snippet below shows an example configuration for executing remotely on OSG.
The configuration uses the `CondorProvider` to interface with the scheduler.

.. note:: This config was last tested with 0.8.0

.. literalinclude:: ../../parsl/configs/osg.py

Amazon Web Services
-------------------

.. image:: ./aws_image.png

.. note::
   Please note that **boto3** library is a requirement to use AWS with Parsl.
   This can be installed via ``python3 -m pip install parsl[aws]``

Amazon Web Services is a commercial cloud service which allows you to rent a range of computers and other computing services.
The snippet below shows an example configuration for provisioning nodes from the Elastic Compute Cloud (EC2) service.
The first run would configure a Virtual Private Cloud and other networking and security infrastructure that will be
re-used in subsequent runs. The configuration uses the `AWSProvider` to connect to AWS.

.. literalinclude:: ../../parsl/configs/ec2.py

Kubernetes Clusters
-------------------

.. image:: https://d1.awsstatic.com/PAC/kuberneteslogo.eabc6359f48c8e30b7a138c18177f3fd39338e05.png

Kubernetes is an open-source system for container management, such as automating deployment and scaling of containers.
The snippet below shows an example configuration for deploying pods as workers on a Kubernetes cluster.
The KubernetesProvider exploits the Python Kubernetes API, which assumes that you have kube config in `~/.kube/config`.

.. literalinclude:: ../../parsl/configs/kubernetes.py


Ad-Hoc Clusters
---------------

Any collection of compute nodes without a scheduler setup for task scheduling can be considered an
ad-hoc cluster. Often these machines have a shared filesystem such as NFS or Lustre.
In order to use these resources with Parsl, they need to set-up for password-less SSH access.

To use these ssh-accessible collection of nodes as an ad-hoc cluster, we create an executor
for each node, using the `LocalProvider` with `SSHChannel` to identify the node by hostname. An example
configuration follows.

.. literalinclude:: ../../parsl/configs/ad_hoc.py

.. note::
   Multiple blocks should not be assigned to each node when using the `HighThroughputExecutor`

.. note::
   Load-balancing will not work properly with this approach. In future work, a dedicated provider
   that supports load-balancing will be implemented. You can follow progress on this work
   `here <https://github.com/Parsl/parsl/issues/941>`_.


Work Queue (CCL ND)
------------------

.. image:: http://ccl.cse.nd.edu/software/workqueue/WorkQueueLogoSmall.png

The following snippet shows an example configuration for using the Work Queue distributed framework to run applications on remote machines at large. This examples uses the `WorkQueueExecutor` to schedule tasks locally, and assumes that Work Queue workers have been externally connected to the master using the `work_queue_worker` or `condor_submit_workers` command line utilities from CCTools. For more information the process of submitting tasks and workers to Work Queue, please refer to the `CCTools Work Queue documentation <https://cctools.readthedocs.io/en/latest/work_queue/>`.

.. literalinclude::  ../../parsl/configs/wqex_local.py

To utilize Work Queue with Parsl, please install the full CCTools software package within an appropriate Anaconda or Miniconda environment (instructions for installing Miniconda can be found `here <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`):

.. codeblock:: bash
    $ conda create -y --name <environment> python=<version>
    $ conda activate <environment>
    $ conda install -y -c conda-forge cctools

This creates a Conda environment on your machine with all the necessary tools and setup needed to utilize Work Queue with the Parsl library. 


Further help
------------

For help constructing a configuration, you can click on class names such as :class:`~parsl.config.Config` or :class:`~parsl.executors.HighThroughputExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the python command line via, for example::

    >>> from parsl.config import Config
    >>> help(Config)

