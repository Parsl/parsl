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

.. contents:: Example Site Configurations

.. note::
   Please note that all configuration examples below import a `user_opts` file where all user specific
   options are defined. To use the configuration, these options **must** be defined either by creating
   a user_opts file, or explicitly edit the configuration with user specific information.

Comet (SDSC)
------------

.. image:: https://ucsdnews.ucsd.edu/news_uploads/comet-logo.jpg

The following snippet shows an example configuration for executing remotely on San Diego Supercomputer Center's **Comet** supercomputer. The example uses an `SSHChannel` to connect remotely to Comet, the `SlurmProvider` to interface with the slurm scheduler used by Comet and the `SrunLauncher` to launch workers.

.. literalinclude:: ../../parsl/tests/configs/comet_ipp_multinode.py


Cori (NERSC)
------------

.. image:: https://6lli539m39y3hpkelqsm3c2fg-wpengine.netdna-ssl.com/wp-content/uploads/2017/08/Cori-NERSC.png

The following snippet shows an example configuration for accessing NERSC's **Cori** supercomputer. This example uses the IPythonParallel executor and connects to Cori's Slurm scheduler. It uses a remote SSH channel that allows the IPythonParallel controller to be hosted on the script's submission machine (e.g., a PC).  It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. literalinclude:: ../../parsl/tests/configs/cori_ipp_multinode.py


Theta (ALCF)
------------

.. image:: https://www.alcf.anl.gov/files/ALCF-Theta_111016-1000px.jpg

The following snippet shows an example configuration for executing on Argonne Leadership Computing Facility's **Theta** supercomputer.
This example uses the `IPythonParallel` executor and connects to Theta's Cobalt scheduler using the `CobaltProvider`. This configuration
assumes that the script is being executed on the login nodes of Theta.

.. literalinclude:: ../../parsl/tests/configs/theta_local_ipp_multinode.py


Cooley (ALCF)
------------

.. image:: https://today.anl.gov/wp-content/uploads/sites/44/2015/06/Cray-Cooley.jpg

The following snippet shows an example configuration for executing remotely on Argonne Leadership Computing Facility's **Cooley** analysis and visualization system.
The example uses an `SSHInteractiveLoginChannel` to connect remotely to Cooley using ALCF's 2FA token.
The configuration uses the `CobaltProvider` to interface with the Cooleys' scheduler.

.. literalinclude:: ../../parsl/tests/configs/cooley_ssh_il_single_node.py

Swan (Cray)
-----------

.. image:: https://www.cray.com/blog/wp-content/uploads/2016/11/XC50-feat-blog.jpg

The following snippet shows an example configuration for executing remotely on Swan, an XC50 machine hosted by the Cray Partner Network.
The example uses an `SSHChannel` to connect remotely Swan, uses the `TorqueProvider` to interface with the scheduler and the `AprunLauncher`
to launch workers on the machine

.. literalinclude:: ../../parsl/tests/configs/swan_ipp_multinode.py



Further help
------------

For help constructing a configuration, you can click on class names such as :class:`~parsl.config.Config` or :class:`~parsl.executors.ipp.IPyParallelExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the python command line via, for example::

    >>> from parsl.config import Config
    >>> help(Config)

