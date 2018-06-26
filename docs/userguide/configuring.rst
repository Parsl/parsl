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


Example
-------

The following shows an example configuration for accessing NERSC's Cori supercomputer. This example uses the IPythonParallel executor and connects to Cori's Slurm scheduler. It uses a remote SSH channel that allows the IPythonParallel controller to be hosted on the script's submission machine (e.g., a PC).  It is configured to request 2 nodes configured with 1 TaskBlock per node. Finally it includes override information to request a particular node type (Haswell) and to configure a specific Python environment on the worker nodes using Anaconda.

.. code-block :: python

    from libsubmit.providers.slurm.slurm import Slurm
    from libsubmit.channels.ssh.ssh import SSHChannel
    from parsl.config import Config
    from parsl.executors.ipp import IPyParallelExecutor

    config = Config(
        executors=[
            IPyParallelExecutor(
                label='cori_ipp_single_node',
                provider=Slurm(
                    'debug',
                    channel=SSHChannel(
                        username='username',
                        hostname='cori.nersc.gov',
                        script_dir='/global/homes/y/username/parsl_scripts'
                    )
                    nodes_per_block=2,
                    tasks_per_node=1,
                    init_blocks=1,
                    max_blocks=1,
                    walltime="00:10:00",
                    overrides="module load python/3.5-anaconda; source activate /global/homes/y/yadunand/.conda/envs/parsl_env_3.5"
                )
            )

        ]
    )

Further help
------------

For help constructing a configuration, you can click on class names such as :class:`~parsl.config.Config` or :class:`~parsl.executors.ipp.IPyParallelExecutor` to see the associated class documentation. The same documentation can be accessed interactively at the python command line via, for example::

    >>> from parsl.config import Config
    >>> help(Config)

