Starting Workers
================

.. note::

   `Select a provider <providers.html>`_ if you have not already.

The next step in defining a **Provider** is configuring how to start Parsl on
newly-acquired resources.
Starting Parsl occurs in two steps, each with their own Provider option:

1. Defining how to connect to each new compute node (``launcher``)
2. Defining how to initialize the computational information (``worker_init``)

Selecting a Launcher
--------------------

Parsl uses **Launchers** to start workers on every node acquired by a Provider.
Launchers are required when requesting more than one node per batch job from a
`cluster scheduler <providers.html#cluster-scheduler>`_.

Parsl will use the same mechanism employed for launching MPI tasks across compute nodes.
Consult the documentation or example scripts for a compute cluster to find how to launch MPI tasks,
and then find the `launcher script which matches <../../reference.html#launchers>`_.

No options are required in most cases.
Simply import the Launcher class and provide an instance of the launcher to the ``launcher`` option of the Provider.

.. code-block:: python

    config = Config(
     executors=[
              ...
                   provider=SlurmProvider(
                        ...
                        launcher=SrunLauncher(),
                        ...
                   ),
              )
         ],
    )

For example, `the Polaris supercomputer at ALCF <https://docs.alcf.anl.gov/polaris/running-jobs/#running-mpiopenmp-applications>`_
suggests to use ``mpiexec`` to start jobs
and Parsl will use the :class:`~parsl.launchers.MpiExecLauncher`.


Advanced: Configuring a Launcher
++++++++++++++++++++++++++++++++

Common cases for configuring a launcher include:

1. Defining binding between the Parsl processes and available cores of the computer,
   as when `using Parsl on the Aurora supercomputer <https://docs.alcf.anl.gov/aurora/workflows/parsl/#parsl-config-for-a-large-ensemble-of-single-tile-tasks-run-over-many-pbs-jobs>`_.
2. Launching Parsl inside of Shifter containers by prepending the command to start
   Parsl with the :class:`~parsl.launchers.WrappedLauncher`.


Worker Initialization
---------------------

The ``worker_init`` argument of all Providers accepts a script for readying a newly-acquired node to run Parsl.
Worker initialization commands typically perform tasks such as:

1. Activating the Python virtual environment which contains Parsl
2. Loading modules and setting environment variables of codes invoked by Parsl Apps
3. Launching services required for proper execution (e.g., port forwarding, NVIDIA MPS Daemons)

Most ``worker_init`` scripts activate a virtual environment then check that the proper
version of Python is available.

.. code-block:: python

    config = Config(
         executors=[
              ...
                   provider=SlurmProvider(
                        ...
                        worker_init="conda activate /path/to/conda/env; which python"
                        ...
                   ),
              )
         ],
    )

.. note::

    Consider including commands which print information about a node (e.g., ``hostname``)
    or the computational environment (e.g., ``echo $PATH``) as part of the worker init
    to aid determining where Parsl is running or whether the init function created the
    desired environment.
