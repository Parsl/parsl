.. _configuration-section:

Configuration
=============

Parsl separates program logic from execution configuration, enabling
programs to be developed entirely independently from their execution
environemnt. Configuration is described by a Python object (:class:`~parsl.config.Config`) 
so that developers can 
introspect permissible options, validate settings, and retrieve/edit
configurations dynamically during execution. A configuration object specifies 
details of the provider, executors, connection channel, allocation size, 
queues, durations, and data management options. 

The following example shows a basic configuration object (config) for the Frontera
supercomputer at TACC.
This config uses the HighThroughputExecutor to submit
tasks from a login node (LocalChannel). It requests an allocation of
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

+---------------------+-------------------------------+------------------------+
| Target              | Executor                      | Provider               |
+=====================+===============================+========================+
| Laptop/Workstation  | * `HighThroughputExecutor`    | `LocalProvider`        |
|                     | * `ThreadPoolExecutor`        |                        |
|                     | * `WorkQueueExecutor` beta_   |                        |
+---------------------+-------------------------------+------------------------+
| Amazon Web Services | * `HighThroughputExecutor`    | `AWSProvider`          |
+---------------------+-------------------------------+------------------------+
| Google Cloud        | * `HighThroughputExecutor`    | `GoogleCloudProvider`  |
+---------------------+-------------------------------+------------------------+
| Slurm based system  | * `ExtremeScaleExecutor`      | `SlurmProvider`        |
|                     | * `HighThroughputExecutor`    |                        |
|                     | * `WorkQueueExecutor` beta_   |                        |
+---------------------+-------------------------------+------------------------+
| Torque/PBS based    | * `ExtremeScaleExecutor`      | `TorqueProvider`       |
| system              | * `HighThroughputExecutor`    |                        |
|                     | * `WorkQueueExecutor` beta_   |                        |
+---------------------+-------------------------------+------------------------+
| Cobalt based system | * `ExtremeScaleExecutor`      | `CobaltProvider`       |
|                     | * `HighThroughputExecutor`    |                        |
|                     | * `WorkQueueExecutor` beta_   |                        |
+---------------------+-------------------------------+------------------------+
| GridEngine based    | * `HighThroughputExecutor`    | `GridEngineProvider`   |
| system              | * `WorkQueueExecutor` beta_   |                        |
+---------------------+-------------------------------+------------------------+
| Condor based        | * `HighThroughputExecutor`    | `CondorProvider`       |
| cluster or grid     | * `WorkQueueExecutor` beta_   |                        |
+---------------------+-------------------------------+------------------------+
| Kubernetes cluster  | * `HighThroughputExecutor`    | `KubernetesProvider`   |
+---------------------+-------------------------------+------------------------+

.. [beta] WorkQueueExecutor is available in `v1.0.0` in beta status.


2.  How many nodes will be used to execute the apps? What task durations are necessary to achieve good performance?


+--------------------------+----------------------+-------------------------------------+
| Executor                 | Number of Nodes [*]_ | Task duration for good performance  |
+==========================+======================+=====================================+
| `ThreadPoolExecutor`     | 1 (Only local)       | Any                                 |
+--------------------------+----------------------+-------------------------------------+
| `HighThroughputExecutor` | <=2000               | Task duration(s)/#nodes >= 0.01     |
|                          |                      | longer tasks needed at higher scale |
+--------------------------+----------------------+-------------------------------------+
| `ExtremeScaleExecutor`   | >1000, <=8000 [*]_   | >minutes                            |
+--------------------------+----------------------+-------------------------------------+
| `WorkQueueExecutor`      | <=1000 [*]_          | 10s+                                |
+--------------------------+----------------------+-------------------------------------+


.. [*] Assuming 32 workers per node. If there are fewer workers launched
       per node, a larger number of nodes could be supported.

.. [*] 8,000 nodes with 32 workers (256,000 workers) is the maximum scale at which
       the `ExtremeScaleExecutor` has been tested.

.. [*] The maximum number of nodes tested for the `WorkQueueExecutor` is 10,000 GPU cores and
       20,000 CPU cores.

.. warning:: `IPyParallelExecutor` is  deprecated as of Parsl v0.8.0. `HighThroughputExecutor`
   is the recommended replacement.


3. Should Parsl request multiple nodes in an individual scheduler job? 
(Here the term block is equivalent to a single scheduler job.)

+----------------------------------------------------------------------------+
| ``nodes_per_block = 1``                                                    |
+---------------------+--------------------------+---------------------------+
| Provider            | Executor choice          | Suitable Launchers        |
+=====================+==========================+===========================+
| Systems that don't  | Any                      | * `SingleNodeLauncher`    |
| use Aprun           |                          | * `SimpleLauncher`        |
+---------------------+--------------------------+---------------------------+
| Aprun based systems | Any                      | * `AprunLauncher`         |
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

.. note:: If using a Cray system, you most likely need to use the `AprunLauncher` to launch workers unless you
          are on a **native Slurm** system like :ref:`configuring_nersc_cori`


4) Where will the main Parsl program run and how will it communicate with the apps?

+------------------------+--------------------------+------------------------------------+
| Parsl program location | App execution target     | Suitable channel                   |
+========================+==========================+====================================+
| Laptop/Workstation     | Laptop/Workstation       | `LocalChannel`                     |
+------------------------+--------------------------+------------------------------------+
| Laptop/Workstation     | Cloud Resources          | No channel is needed               |
+------------------------+--------------------------+------------------------------------+
| Laptop/Workstation     | Clusters with no 2FA     | `SSHChannel`                       |
+------------------------+--------------------------+------------------------------------+
| Laptop/Workstation     | Clusters with 2FA        | `SSHInteractiveLoginChannel`       |
+------------------------+--------------------------+------------------------------------+
| Login node             | Cluster/Supercomputer    | `LocalChannel`                     |
+------------------------+--------------------------+------------------------------------+


Ad-Hoc Clusters
---------------

Any collection of compute nodes without a scheduler can be considered an
ad-hoc cluster. Often these machines have a shared file system such as NFS or Lustre.
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


Amazon Web Services
-------------------

.. image:: ./aws_image.png

.. note::
   Please note that **boto3** library must be installed to use AWS with Parsl.
   This can be installed via ``python3 -m pip install parsl[aws]``

Amazon Web Services is a commercial cloud service which allows users to rent a range of computers and other computing services.
The following snippet shows how Parsl can be configured to provision nodes from the Elastic Compute Cloud (EC2) service.
The first time this configuration is used, Parsl will configure a Virtual Private Cloud and other networking and security infrastructure that will be
re-used in subsequent executions. The configuration uses the `AWSProvider` to connect to AWS.

.. literalinclude:: ../../parsl/configs/ec2.py


ASPIRE 1 (NSCC)
---------------

.. image:: https://www.nscc.sg/wp-content/uploads/2017/04/ASPIRE1Img.png

The following snippet shows an example configuration for accessing NSCC's **ASPIRE 1** supercomputer. This example uses the `HighThroughputExecutor` executor and connects to ASPIRE1's PBSPro scheduler. It also shows how `scheduler_options` parameter could be used for scheduling array jobs in PBSPro.

.. literalinclude:: ../../parsl/configs/ASPIRE1.py


Blue Waters (NCSA)
------------------

.. image:: https://www.cray.com/sites/default/files/images/Solutions_Images/bluewaters.png

The following snippet shows an example configuration for executing remotely on Blue Waters, a flagship machine at the National Center for Supercomputing Applications.
The configuration assumes the user is running on a login node and uses the `TorqueProvider` to interface
with the scheduler, and uses the `AprunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/bluewaters.py


CC-IN2P3
--------

.. image:: https://cc.in2p3.fr/wp-content/uploads/2017/03/bandeau_accueil.jpg

The snippet below shows an example configuration for executing from a login node on IN2P3's Computing Centre.
The configuration uses the `LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support yet.
This system uses Grid Engine which Parsl interfaces with using the `GridEngineProvider`.

.. literalinclude:: ../../parsl/configs/cc_in2p3.py


CCL (Notre Dame, with Work Queue)
---------------------------------

.. image:: http://ccl.cse.nd.edu/software/workqueue/WorkQueueLogoSmall.png

The following snippet shows an example configuration for using the Work Queue distributed framework to run applications on remote machines at large. This examples uses the `WorkQueueExecutor` to schedule tasks locally, and assumes that Work Queue workers have been externally connected to the master using the `work_queue_worker` or `condor_submit_workers` command line utilities from CCTools. For more information the process of submitting tasks and workers to Work Queue, please refer to the `CCTools Work Queue documentation <https://cctools.readthedocs.io/en/latest/work_queue/>`.

.. literalinclude::  ../../parsl/configs/wqex_local.py

To utilize Work Queue with Parsl, please install the full CCTools software package within an appropriate Anaconda or Miniconda environment (instructions for installing Miniconda can be found `here <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`):

.. code-block:: bash

   $ conda create -y --name <environment> python=<version>
   $ conda activate <environment>
   $ conda install -y -c conda-forge cctools

This creates a Conda environment on your machine with all the necessary tools and setup needed to utilize Work Queue with the Parsl library.


Comet (SDSC)
------------

.. image:: https://ucsdnews.ucsd.edu/news_uploads/comet-logo.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer
Center's **Comet** supercomputer. The example is designed to be executed on the login nodes, using the
`SlurmProvider` to interface with the Slurm scheduler used by Comet and the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/comet.py


Cooley (ALCF)
-------------

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's
**Cooley** analysis and visualization system.
The example uses the `HighThroughputExecutor` and connects to Cooley's Cobalt scheduler
using the `CobaltProvider`. This configuration assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/cooley.py


.. _configuring_nersc_cori:

Cori (NERSC)
------------

.. image:: https://6lli539m39y3hpkelqsm3c2fg-wpengine.netdna-ssl.com/wp-content/uploads/2017/08/Cori-NERSC.png

The following snippet shows an example configuration for accessing NERSC's **Cori** supercomputer. This example uses the `HighThroughputExecutor` and connects to Cori's Slurm scheduler.
It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. literalinclude:: ../../parsl/configs/cori.py


Frontera (TACC)
---------------

.. image:: https://fronteraweb.tacc.utexas.edu/media/filer_public/e2/66/e266466f-502e-4bfe-92d6-3634d697ed99/frontera-home.jpg

Deployed in June 2019, Frontera is the 5th most powerful supercomputer in the world. Frontera replaces the NSF Blue Waters system at NCSA
and is the first deployment in the National Science Foundation's petascale computing program. The configuration below assumes that the user is
running on a login node and uses the `SlurmProvider` to interface with the scheduler, and uses the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/frontera.py


Kubernetes Clusters
-------------------

.. image:: https://d1.awsstatic.com/PAC/kuberneteslogo.eabc6359f48c8e30b7a138c18177f3fd39338e05.png

Kubernetes is an open-source system for container management, such as automating deployment and scaling of containers.
The snippet below shows an example configuration for deploying pods as workers on a Kubernetes cluster.
The KubernetesProvider exploits the Python Kubernetes API, which assumes that you have kube config in `~/.kube/config`.

.. literalinclude:: ../../parsl/configs/kubernetes.py


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


Stampede2 (TACC)
----------------

.. image:: https://www.tacc.utexas.edu/documents/1084364/1413880/stampede2-0717.jpg/

The following snippet shows an example configuration for accessing TACC's **Stampede2** supercomputer. This example uses theHighThroughput executor and connects to Stampede2's Slurm scheduler.

.. literalinclude:: ../../parsl/configs/stampede2.py


Summit (ORNL)
-------------

.. image:: https://www.olcf.ornl.gov/wp-content/uploads/2018/06/Summit_Exaop-1500x844.jpg

The following snippet shows an example configuration for executing from the login node on Summit, the leadership class supercomputer hosted at the Oak Ridge National Laboratory.
The example uses the `LSFProvider` to provision compute nodes from the LSF cluster scheduler and the `JsrunLauncher` to launch workers across the compute nodes.

.. literalinclude:: ../../parsl/configs/summit.py


Theta (ALCF)
------------

.. image:: https://www.alcf.anl.gov/files/ALCF-Theta_111016-1000px.jpg

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's
**Theta** supercomputer. This example uses the `HighThroughputExecutor` and connects to Theta's Cobalt scheduler
using the `CobaltProvider`. This configuration assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/theta.py


Further help
------------

For help constructing a configuration, you can click on class names such as :class:`~parsl.config.Config` or :class:`~parsl.executors.HighThroughputExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the python command line via, for example::

    >>> from parsl.config import Config
    >>> help(Config)
