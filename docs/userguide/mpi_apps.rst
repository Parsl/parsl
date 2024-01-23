MPI Apps
========

.. note::

    Parsl's support for MPI Apps described below is pending release.
    Please use the ``mpi_experimental_3`` branch to use the functionality
    described in this document. To install directly from github:

    >>  pip install git+https://github.com/Parsl/parsl.git@mpi_experimental_3

MPI applications run multiple copies of a program that complete a single task by
coordinating using messages passed within or across nodes.
Starting MPI application requires invoking a "launcher" code (e.g., ``mpiexec``) from one node
with options that define how the copies of a program should be distributed to others.
Parsl simplifies this by composing the "launcher" command from the resources specified at the time
each app is invoked.

The broad strokes of a complete solution involves the following components:

1. Configuring the :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` with:
    ``enable_mpi_mode=True``
2. Specify an MPI Launcher from one of the supported launchers ("aprun", "srun", "mpiexec") for the
   :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` with: ``mpi_launcher="srun"``
3. Specify the provider that matches your cluster, (eg. user ``SlurmProvider`` for Slurm clusters)
4. Set the non-mpi launcher to :class:`~parsl.launchers.SimpleLauncher`
5. Specify resources required by the application via ``resource_specification`` as shown below:


.. code-block:: python

    # Define HighThroughputExecutor(enable_mpi_mode=True, mpi_launcher="mpiexec", ...)

    @bash_app
    def lammps_mpi_application(infile: File, parsl_resource_specification: Dict):
        # PARSL_MPI_PREFIX will resolve to `mpiexec -n 4 -ppn 2 -hosts NODE001,NODE002`
        return f"$PARSL_MPI_PREFIX lmp_mpi -in {infile.filepath}"

    # Resources in terms of nodes and how ranks are to be distributed are set on a per app
    # basis via the resource_spec dictionary.
    resource_spec = {
        "NUM_NODES" = 2,
        "RANKS_PER_NODE" = 2,
    }
    future = lammps_mpi_application(File('in.file'), resource_specification=resource_spec)


HTEX and MPI Tasks
------------------

The :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` (HTEX) is the
default executor available through Parsl.
Parsl Apps which invoke MPI code require MPI specific configuration such that:

1. All workers are started on the lead-node (mom-node in case of Crays)
2. Resource requirements of Apps are propagated to workers who provision the required number of nodes from within the batch job.


Configuring the Provider
++++++++++++++++++++++++

Parsl must be configured to deploy workers on exactly one node per block. This part is
simple. Instead of defining a launcher which will place an executor on each node in the
block, simply use the :class:`~parsl.launchers.SimpleLauncher`.
The MPI Launcher that the application will use is to be specified via ``HighThroughputExecutor(mpi_launcher="LAUNCHER")``

It is also necessary to specify the desired number of blocks for the executor.
Parsl cannot determine the number of blocks needed to run a set of MPI Tasks,
so they must bet set explicitly (see `Issue #1647 <https://github.com/Parsl/parsl/issues/1647>`_).
The easiest route is to set the ``max_blocks`` and ``min_blocks`` of the provider
to the desired number of blocks.

Configuring the Executor
++++++++++++++++++++++++

Here are the steps for configuring the executor:

1. Set ``HighThroughputExecutor(enable_mpi_mode=True)``
2. Set ``HighThroughputExecutor(mpi_launcher="LAUNCHER")`` to one from ("srun", "aprun", "mpiexec")
3. Set the ``max_workers`` to the number of MPI Apps you expect to run per scheduler job (block).
4. Set ``cores_per_worker=1e-6`` to prevent HTEx from reducing the number of workers if you request more workers than cores.


Example Configuration
~~~~~~~~~~~~~~~~~~~~~

Here's an example configuration which runs MPI tasks on ALCF's Polaris Supercomputer

.. code-block:: python

    import parsl
    from typing import Dict
    from parsl.config import Config

    # PBSPro is the right provider for Polaris:
    from parsl.providers import PBSProProvider
    # The high throughput executor is for scaling to HPC systems:
    from parsl.executors import HighThroughputExecutor
    # You can use the MPI launcher, but may want the Gnu Parallel launcher, see below
    from parsl.launchers import MpiExecLauncher, GnuParallelLauncher, SimpleLauncher
    # address_by_interface is needed for the HighThroughputExecutor:
    from parsl.addresses import address_by_interface
    # For checkpointing:
    from parsl.utils import get_all_checkpoints

    # Adjust your user-specific options here:
    # run_dir="/lus/grand/projects/yourproject/yourrundir/"

    user_opts = {
        "worker_init": "module load conda; conda activate parsl_mpi_py310",
        "scheduler_options":"#PBS -l filesystems=home:eagle:grand\n#PBS -l place=scatter" ,
        "account": SET_YOUR_ALCF_ALLOCATION_HERE,
        "queue":  "debug-scaling",
        "walltime":  "1:00:00",
        "nodes_per_block":  8,
        "available_accelerators": 4, # Each Polaris node has 4 GPUs, setting this ensures one worker per GPU
        "cores_per_worker": 8, # this will set the number of cpu hardware threads per worker.
    }

    config = Config(
            executors=[
                HighThroughputExecutor(
                    label="htex",
                    enable_mpi_mode=True,
                    mpi_launcher="mpiexec",
                    cores_per_worker=user_opts["cores_per_worker"],
                    address=address_by_interface("bond0"),
                    provider=PBSProProvider(
                        launcher=SimpleLauncher(),
                        account=user_opts["account"],
                        queue=user_opts["queue"],
                        # PBS directives (header lines): for array jobs pass '-J' option
                        scheduler_options=user_opts["scheduler_options"],
                        # Command to be run before starting a worker, such as:
                        worker_init=user_opts["worker_init"],
                        # number of compute nodes allocated for each block
                        nodes_per_block=user_opts["nodes_per_block"],
                        init_blocks=1,
                        min_blocks=0,
                        max_blocks=1, # Can increase more to have more parallel jobs
                        walltime=user_opts["walltime"]
                    ),
                ),
            ],


Writing MPI-Compatible Apps
++++++++++++++++++++++++++++

In MPI mode, the :class:`~parsl.executors.high_throughput.executor.HighThroughputExecutor` can execute both Python or Bash Apps which invokes the MPI application.
However, it is important to not that Python Apps that directly use ``mpi4py`` is not supported.

For multi-node MPI applications, especially when running multiple applications within a single batch job,
it is important to specify the resource requirements for the app so that the Parsl worker can provision
the appropriate resources before the application starts. For eg, your Parsl script might contain a molecular
dynamics application that requires 8 ranks over 1 node for certain inputs and 32 ranks over 4 nodes for some
depending on the size of the molecules being simulated. By specifying resources via ``resource_specification``,
parsl workers will provision the requested resources and then compose MPI launch command prefixes
(Eg: ``mpiexec -n <ranks> -ppn <ranks_per_node> -hosts <node1..nodeN>``). These launch command prefixes are
shared with the app via environment variables.

.. code-block:: python

    @bash_app
    def echo_hello(n: int, stderr='std.err', stdout='std.out', parsl_resource_specification: Dict):
        return f'$PARSL_MPI_PREFIX hostname'

    # The following app will echo the hostname from several MPI ranks
    # Alternatively, you could also use the resource_specification to compose a launch
    # command using env vars set by Parsl from the resource_specification like this:
    @bash_app
    def echo_hostname(n: int, stderr='std.err', stdout='std.out', parsl_resource_specification: Dict):
        total_ranks = os.environ("")
        return f'aprun -N $PARSL_RANKS_PER_NODE -n {total_ranks} /bin/hostname'


All valid key-value pairs set in the resource_specification are exported to the application via env vars,
for eg. ``parsl_resource_specification = {'RANKS_PER_NODE': 4} `` will set the env var ``PARSL_RANKS_PER_NODE``

However, the following options are **required** for MPI applications :

.. code-block:: python

    resource_specification = {
      'NUM_NODES': <int>,        # Number of nodes required for the application instance
      'RANKS_PER_NODE': <int>,   # Number of Ranks / application elements to be launched per node
    }

    # The above are made available in the worker env vars:
    # echo $PARSL_NUM_NODES, $PARSL_RANKS_PER_NODE

When the above are supplied, the following launch command prefixes are set:

.. code-block::

    PARSL_MPIEXEC_PREFIX: mpiexec launch command which works for a large number of batch systems especially PBS systems
    PARSL_SRUN_PREFIX: srun launch command for Slurm based clusters
    PARSL_APRUN_PREFIX: aprun launch command prefix for some Cray machines
    PARSL_MPI_PREFIX: Parsl sets the MPI prefix to match the mpi_launcher specified to `HighThroughputExecutor`
    PARSL_MPI_NODELIST: List of assigned nodes separated by commas (Eg, NODE1,NODE2)
    PARSL_WORKER_POOL_ID: Alphanumeric string identifier for the worker pool
    PARSL_WORKER_BLOCK_ID: Batch job ID that the worker belongs to


Example Application: CosmicTagger
+++++++++++++++++++++++++++++++++

TODO: Blurb about what CosmicTagger does
CosmicTagger implements models and training utilities to train convolutional networks to
separate cosmic pixels, background pixels, and neutrino pixels in a neutrinos dataset.
There are several variations. A detailed description of the code can be found in:

`Cosmic Background Removal with Deep Neural Networks in SBND <https://www.frontiersin.org/articles/10.3389/frai.2021.649917/full>`_

Cosmic Background Removal with Deep Neural Networks in SBND
This network is implemented in both PyTorch and TensorFlow. To select between the networks, you can use the --framework parameter. It accepts either tensorflow or torch. The model is available in a development version with sparse convolutions in the torch framework.

This example is broken down into three components. First, configure the Executor for Polaris at
ALCF. The configuration will use the :class:`~parsl.providers.PBSProProvider` to connect to the batch scheduler.
With the goal of running MPI applications, we set the

.. code-block:: python

    import parsl
    from typing import Dict
    from parsl.config import Config

    # PBSPro is the right provider for Polaris:
    from parsl.providers import PBSProProvider
    # The high throughput executor is for scaling to HPC systems:
    from parsl.executors import HighThroughputExecutor
    # address_by_interface is needed for the HighThroughputExecutor:
    from parsl.addresses import address_by_interface

    user_opts = {
        # Make sure to setup a conda environment before using this config
        "worker_init": "module load conda; conda activate parsl_mpi_py310",
        "scheduler_options":"#PBS -l filesystems=home:eagle:grand\n#PBS -l place=scatter" ,
        "account": <SET_YOUR_ALLOCATION>,
        "queue":  "debug-scaling",
        "walltime":  "1:00:00",
        "nodes_per_block":  8,
        "available_accelerators": 4, # Each Polaris node has 4 GPUs, setting this ensures one worker per GPU
        "cores_per_worker": 8, # this will set the number of cpu hardware threads per worker.
    }

    config = Config(
            executors=[
                HighThroughputExecutor(
                    label="htex",
                    enable_mpi_mode=True,
                    mpi_launcher="mpiexec",
                    cores_per_worker=user_opts["cores_per_worker"],
                    address=address_by_interface("bond0"),
                    provider=PBSProProvider(
                        account=user_opts["account"],
                        queue=user_opts["queue"],
                        # PBS directives (header lines): for array jobs pass '-J' option
                        scheduler_options=user_opts["scheduler_options"],
                        # Command to be run before starting a worker, such as:
                        worker_init=user_opts["worker_init"],
                        # number of compute nodes allocated for each block
                        nodes_per_block=user_opts["nodes_per_block"],
                        init_blocks=1,
                        min_blocks=0,
                        max_blocks=1, # Can increase more to have more parallel jobs
                        walltime=user_opts["walltime"]
                    ),
                ),
            ],
    )



Next we define the CosmicTagger MPI application. TODO: Ask Khalid for help.

.. code-block:: python

    @parsl.bash_app
    def cosmic_tagger(workdir: str,
                      datatype: str = "float32",
                      batchsize: int = 8,
                      framework: str = "torch",
                      iterations: int = 500,
                      trial: int = 2,
                      stdout=parsl.AUTO_LOGNAME,
                      stderr=parsl.AUTO_LOGNAME,
                      parsl_resource_specification:Dict={}):
        NRANKS = parsl_resource_specification['NUM_NODES'] * parsl_resource_specification['RANKS_PER_NODE']

        return f"""
        module purge
        module use /soft/modulefiles/
        module load conda/2023-10-04
        conda activate

        echo "PARSL_MPI_PREFIX : $PARSL_MPI_PREFIX"

        $PARSL_MPI_PREFIX --cpu-bind numa \
            python {workdir}/bin/exec.py --config-name a21 \
                run.id=run_plrs_ParslDemo_g${NRANKS}_{datatype}_b{batchsize}_{framework}_i{iterations}_T{trial} \
                run.compute_mode=GPU \
                run.distributed=True \
                framework={framework} \
                run.minibatch_size={batchsize} \
                run.precision={datatype} \
                mode.optimizer.loss_balance_scheme=light \
                run.iterations={iterations}
        """

In this example, we run a simple test that does an exploration over the ``batchsize`` parameter
while launching the application over 2-4 nodes.

.. code-block:: python

    def run_cosmic_tagger():
        futures = {}
        for num_nodes in [2, 4]:
            for batchsize in [2, 4, 8]:

                parsl_res_spec = {"NUM_NODES": num_nodes,
                                  "RANKS_PER_NODE": 4}
                future = cosmic_tagger(workdir="/home/yadunand/CosmicTagger",
                                       datatype="float32",
                                       batchsize=str(batchsize),
                                       parsl_resource_specification=parsl_res_spec)


                print(f"Stdout : {future.stdout}")
                print(f"Stderr : {future.stderr}")
                futures[(num_nodes, batchsize)] = future


        for key in futures:
            print(f"Got result for {key}: {futures[key].result()}")


        run_cosmic_tagger()



Limitations
+++++++++++

Support for MPI tasks in HTEX is limited. It is designed for running many multi-node MPI applications within a single
batch job.

#. MPI tasks may not span across nodes from more than one block.
#. Parsl does not correctly determine the number of execution slots per block (`Issue #1647 <https://github.com/Parsl/parsl/issues/1647>`_)
#. The executor uses a Python process per task, which can use a lot of memory (`Issue #2264 <https://github.com/Parsl/parsl/issues/2264>`_)