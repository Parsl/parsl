.. _configuration-section:

Configuration
=============

Parsl workflows are developed completely independently from their execution environment. Parsl offers an extensible configuration model through which the execution environment and communication with that environment is configured. Parsl is configured using :class:`~parsl.config.Config` object. For more information, see the :class:`~parsl.config.Config` class documentation. The following shows how the configuration can be loaded.

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
   Please note that all configuration examples below import a ``user_opts`` file where all user specific
   options are defined. To use the configuration, these options **must** be defined either by creating
   a user_opts file, or explicitly edit the configuration with user specific information.

How-to Configure
----------------

The configuration provided to Parsl dictates the shape and limits of various resources to be provisioned
for the workflow. Therefore it is important to carefully evaluate certain aspects of the workflow and
the planned compute resources to determine an ideal configuration match.

Here are a series of question to help formulate a suitable configuration:


1. Where would you like the tasks that comprise the workflow to execute?

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

2. How many and how long are the tasks? How many nodes do you have to execute them ?

+---------------------+---------------------+--------------------+----------------------------+
| Node scale          | Task Duration       |  Task Count        | Suitable Executor          |
+=====================+=====================+====================+============================+
| Nodes=1             | <1s - minutes       |  0-100K            | * `ThreadPoolExecutor`     |
|                     |                     |                    | * `HighThroughputExecutor` |
+---------------------+---------------------+--------------------+----------------------------+
| 1<=Nodes<=1000      | <1s - minutes       |  0-1M              | * `ThreadPoolExecutor`     |
|                     |                     |                    | * `IPyParallelExecutor`    |
|                     |                     |                    | * `HighThroughputExecutor` |
+---------------------+---------------------+--------------------+----------------------------+
| Nodes>1000          |  >minutes           |  0-1M              | * `ExtremeScaleExecutor`   |
+---------------------+---------------------+--------------------+----------------------------+

3. If you are running on a cluster or supercomputer, will you request multiple nodes per block ?
   Note that in this case a block is equivalent to a batch job.

+----------------------------------------------------------------------------+
| ``nodes_per_block = 1``                                                    |
+---------------------+--------------------------+---------------------------+
| Provider            | Executor choice          | Suitable Launchers        |
+=====================+==========================+===========================+
| Any except systems  | Any                      | * `SingleNodeLauncher`    |
| using Aprun         |                          | * `SimpleLauncher`        |
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


4. Where will you run the main parsl process vs the tasks?

+---------------------+--------------------------+------------------------------------+
| Workflow location   | Execution target         | Suitable channel                   |
+=====================+==========================+====================================+
| Laptop/Workstation  | Laptop/Workstation       | `LocalChannel`                     |
+---------------------+--------------------------+------------------------------------+
| Laptop/Workstation  | Cloud Resources          | None                               |
+---------------------+--------------------------+------------------------------------+
| Laptop/Workstation  | Clusters with no 2FA     | `SSHChannel`                       |
+---------------------+--------------------------+------------------------------------+
| Laptop/Workstation  | Clusters with 2FA        | `SSHInteractiveLoginChannel`       |
+---------------------+--------------------------+------------------------------------+
| Login node          | Cluster/Supercomputer    | `LocalChannel`                     |
+---------------------+--------------------------+------------------------------------+


Comet (SDSC)
------------

.. image:: https://ucsdnews.ucsd.edu/news_uploads/comet-logo.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer Center's **Comet** supercomputer. The example uses an `SSHChannel` to connect remotely to Comet, the `SlurmProvider` to interface with the Slurm scheduler used by Comet and the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/comet_ipp_multinode.py


.. _configuring_nersc_cori:

Cori (NERSC)
------------

.. image:: https://6lli539m39y3hpkelqsm3c2fg-wpengine.netdna-ssl.com/wp-content/uploads/2017/08/Cori-NERSC.png

The following snippet shows an example configuration for accessing NERSC's **Cori** supercomputer. This example uses the IPythonParallel executor and connects to Cori's Slurm scheduler. It uses a remote SSH channel that allows the IPythonParallel controller to be hosted on the script's submission machine (e.g., a PC).  It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. literalinclude:: ../../parsl/configs/cori_ipp_multinode.py


Theta (ALCF)
------------

.. image:: https://www.alcf.anl.gov/files/ALCF-Theta_111016-1000px.jpg

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's **Theta** supercomputer.
This example uses the `IPyParallelExecutor` and connects to Theta's Cobalt scheduler using the `CobaltProvider`. This configuration
assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/configs/theta_local_ipp_multinode.py


Cooley (ALCF)
-------------

.. image:: https://today.anl.gov/wp-content/uploads/sites/44/2015/06/Cray-Cooley.jpg

The following snippet shows an example configuration for executing remotely on Argonne Leadership Computing Facility's **Cooley** analysis and visualization system.
The example uses an `SSHInteractiveLoginChannel` to connect remotely to Cooley using ALCF's 2FA token.
The configuration uses the `CobaltProvider` to interface with Cooley's scheduler.

.. literalinclude:: ../../parsl/configs/cooley_ssh_il_single_node.py

Swan (Cray)
-----------

.. image:: https://www.cray.com/blog/wp-content/uploads/2016/11/XC50-feat-blog.jpg

The following snippet shows an example configuration for executing remotely on Swan, an XC50 machine hosted by the Cray Partner Network.
The example uses an `SSHChannel` to connect remotely Swan, uses the `TorqueProvider` to interface with the scheduler and the `AprunLauncher`
to launch workers on the machine

.. literalinclude:: ../../parsl/configs/swan_ipp_multinode.py


CC-IN2P3
--------

.. image:: https://cc.in2p3.fr/wp-content/uploads/2017/03/bandeau_accueil.jpg

The snippet below shows an example configuration for executing from a login node on IN2P3's Computing Centre.
The configuration uses the `LocalProvider` to run on a login node primarily to avoid GSISSH, which Parsl does not support yet.
This system uses Grid Engine which Parsl interfaces with using the `GridEngineProvider`.

.. literalinclude:: ../../parsl/configs/cc_in2p3_local_single_node.py

Midway (RCC, UChicago)
----------------------

.. image:: https://rcc.uchicago.edu/sites/rcc.uchicago.edu/files/styles/slideshow-image/public/uploads/images/slideshows/20140430_RCC_8978.jpg?itok=BmRuJ-wq

This Midway cluster is a campus cluster hosted by the Research Computing Center at the University of Chicago.
The snippet below shows an example configuration for executing remotely on Midway.
The configuration uses the `SSHChannel` to connect remotely to Midway, uses the `SlurmProvider` to interface
with the scheduler, and uses the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/configs/midway_ipp_multinode.py


Open Science Grid
-----------------

.. image:: https://hcc-docs.unl.edu/download/attachments/11635314/Screen%20Shot%202013-03-19%20at%202.19.28%20PM.png?version=1&modificationDate=1492720049000&api=v2

The Open Science Grid (OSG) is a national, distributed computing Grid spanning over 100 individual sites to provide tens of thousands of CPU cores.
The snippet below shows an example configuration for executing remotely on OSG.
The configuration uses the `SSHChannel` to connect remotely to OSG, uses the `CondorProvider` to interface
with the scheduler.

.. literalinclude:: ../../parsl/configs/osg_ipp_multinode.py

Amazon Web Services
-------------------

.. image:: ./aws_image.png

.. note::
   Please note that **boto3** library is a requirement to use AWS with Parsl.
   This can be installed via ``python3 -m pip install parsl[aws]``

Amazon Web services is a commercial cloud service which allows you to rent a range of computers and other computing services.
The snippet below shows an example configuration for provisioning nodes from the Elastic Compute Cloud (EC2) service.
The first run would configure a Virtual Private Cloud and other networking and security infrastructure that will be
re-used in subsequent runs. The configuration uses the `AWSProvider` to connect to AWS

.. literalinclude:: ../../parsl/configs/ec2_single_node.py


Ad-Hoc Clusters
---------------

Any collection of compute nodes without a scheduler setup for task scheduling can be considered an
ad-hoc cluster. Often these machines have a shared-filesystem such as NFS or Lustre.
In order to use these resources with Parsl, they need to set-up for password-less SSH access.

In order to use these ssh-accessible collection of nodes as an ad-hoc cluster, we create an executor
for each node, using the `LocalProvider` with `SSHChannel` to identify the node by hostname.
Here's an example configuration using the 2 login nodes from the Midway cluster as a proxy for
an ad-hoc cluster.

.. literalinclude:: ../../parsl/configs/ad_hoc.py

.. note::
   Multiple blocks should not be assigned to each node when using the `HighThroughputExecutor`





Further help
------------

For help constructing a configuration, you can click on class names such as :class:`~parsl.config.Config` or :class:`~parsl.executors.ipp.IPyParallelExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the python command line via, for example::

    >>> from parsl.config import Config
    >>> help(Config)

