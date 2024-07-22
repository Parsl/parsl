6. Configuration
================

Parsl configuration is a crucial aspect of effectively utilizing its capabilities. It allows you to tailor Parsl's behavior to your specific computing environment and the requirements of your parallel workflows. This section will guide you through creating and using configuration objects, configuring Parsl for different environments, and understanding the various options available.

Creating and Using Config Objects
---------------------------------

A Parsl configuration is defined using a `Config` object from the `parsl.config` module. This object encapsulates various settings that control Parsl's execution environment, resource allocation, and data management.

Here's a basic example of creating a Config object:

.. code-block:: python

   from parsl.config import Config
   from parsl.executors import ThreadPoolExecutor

   config = Config(
       executors=[ThreadPoolExecutor(max_threads=4)]
   )

This configuration sets up Parsl to run on your local machine, utilizing up to four threads in parallel for task execution.

Once you've created a `Config` object, you can load it into Parsl using the `parsl.load(config)` function. This initializes the Parsl runtime with your specified configuration.

Configuring for Different Environments
--------------------------------------

Parsl's flexibility allows it to run in diverse environments, from local machines to large-scale clusters and cloud platforms. Each environment may have different resources and constraints, requiring specific configurations.

Local Machines
^^^^^^^^^^^^^^

For running Parsl on your local machine, you can use the `ThreadPoolExecutor` as shown in the basic example above. This executor allows you to run tasks concurrently on multiple threads of your local CPU.

Clusters and Supercomputers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're running Parsl on a cluster or supercomputer, you'll typically use executors like `HighThroughputExecutor` (for high-throughput computing) or `ExtremeScaleExecutor` (for extreme-scale computing). These executors interact with job schedulers like Slurm, Torque, or Cobalt to submit and manage jobs on the cluster nodes.

Cloud Platforms
^^^^^^^^^^^^^^^

Parsl can also run on cloud platforms like Amazon Web Services (AWS) or Google Cloud Platform (GCP). For these environments, you'll use executors like `IPyParallelExecutor` (for interactive parallel computing) or `HighThroughputExecutor` (with a cloud provider). These executors handle the details of provisioning and managing virtual machines on the cloud platform.

Configuration Options
---------------------

Parsl's configuration options can be broadly categorized into the following:

Executors
^^^^^^^^^

Executors are responsible for running your Parsl apps on the available resources. Parsl provides several built-in executors:

- `ThreadPoolExecutor`: For running apps on multiple threads of your local CPU.
- `ProcessPoolExecutor`: For running apps in separate processes on your local machine.
- `HighThroughputExecutor`: For running apps on clusters or supercomputers using job schedulers.
- `ExtremeScaleExecutor`: For running large-scale apps on supercomputers with high concurrency.
- `IPyParallelExecutor`: For running apps in parallel using the IPython parallel computing framework.

You can choose the appropriate executor based on your environment and the type of tasks you want to run.

Providers
^^^^^^^^^

Providers are used to access and manage computing resources. Parsl supports various providers, including:

- `LocalProvider`: For running Parsl on your local machine.
- `SlurmProvider`: For running Parsl on clusters using the Slurm job scheduler.
- `TorqueProvider`: For running Parsl on clusters using the Torque job scheduler.
- `CobaltProvider`: For running Parsl on clusters using the Cobalt job scheduler.
- `AWSProvider`: For running Parsl on Amazon Web Services (AWS).
- `GCPProvider`: For running Parsl on Google Cloud Platform (GCP).

You can configure the provider settings to specify details like the type of virtual machines to use, the number of nodes, and the authentication credentials.

Launchers
^^^^^^^^^

Launchers are responsible for starting the worker processes that execute your Parsl apps. Parsl provides several launchers, such as:

- `SingleNodeLauncher`: For launching workers on a single node.
- `MpiRunLauncher`: For launching MPI applications.
- `SrunLauncher`: For launching workers using the `srun` command in Slurm.

You can choose the appropriate launcher based on your environment and the type of executors you're using.

Other Options
^^^^^^^^^^^^^

In addition to executors, providers, and launchers, Parsl's configuration also includes options for:

- **Monitoring**: You can enable monitoring to track the progress of your workflows and collect statistics.
- **Logging**: You can configure the logging level and format to control how much information Parsl logs.
- **Data management**: You can configure Parsl's data staging mechanisms to optimize data transfer between tasks.
- **App caching**: You can enable app caching to reuse the results of previously executed apps, improving performance for repeated tasks.
- **Checkpointing**: You can enable checkpointing to save the state of your workflows and resume them later in case of failures.
- **Security**: You can configure encryption and authentication options to secure your Parsl communications.

Steps for Configuring Parsl on 10 Common Supercomputers
-------------------------------------------------------

Polaris (ALCF):
^^^^^^^^^^^^^^^

- **Load Modules**: Before you start, load the necessary modules for Parsl and your Python environment. This is usually done with a command like `module load conda` followed by `conda activate parsl_mpi_py310`.
- **Configuration**: Create a `Config` object.
  - Use `HighThroughputExecutor` to run many tasks in parallel.
  - Set `enable_mpi_mode` to `True` if you're running MPI applications.
  - Specify the `mpi_launcher` (e.g., "mpiexec").
  - Set `cores_per_worker` to the number of CPU threads per worker (e.g., 8).
  - Use `PBSProProvider` to connect to the scheduler.
  - Set your project allocation account, the queue (e.g., "debug-scaling"), and the maximum walltime (e.g., "1:00:00").
  - Specify the number of nodes per block (e.g., 8).

Frontera (TACC):
^^^^^^^^^^^^^^^^

- **Configuration**: Create a `Config` object.
  - Use the `HighThroughputExecutor`.
  - Set `max_workers_per_node` to 1.
  - Use `SlurmProvider` to connect to the scheduler.
  - Set your partition name (e.g., "normal") and your project allocation account.
  - Specify the walltime (e.g., "00:10:00").
  - Use `SrunLauncher` to launch workers.

Summit (ORNL):
^^^^^^^^^^^^^^

- **Configuration**: Create a `Config` object.
  - Use the `HighThroughputExecutor`.
  - Set working directory to shared filesystem.
  - Use `LSFProvider` to connect to the scheduler.
  - Use `JsrunLauncher` to launch workers.
  - Set the walltime (e.g., "00:10:00"), number of nodes per block (e.g., 2), and your project allocation.

Stampede2 (TACC):
^^^^^^^^^^^^^^^^^

- **Configuration**: Create a `Config` object.
  - Use the `HighThroughputExecutor`.
  - Set `max_workers_per_node` to 2.
  - Use `SlurmProvider` to connect to the scheduler.
  - Set your partition name and any scheduler options.
  - Use `SrunLauncher` to launch workers.
  - Set the walltime (e.g., "00:30:00").
  - If using Globus for data transfer, configure `GlobusStaging`.

Midway (RCC, UChicago):
^^^^^^^^^^^^^^^^^^^^^^^

- **Configuration**: Create a `Config` object.
  - Use the `HighThroughputExecutor`.
  - Set `max_workers_per_node` to 2.
  - Use `SlurmProvider` to connect to the scheduler.
  - Set your partition name (e.g., "broadwl") and any scheduler options.
  - Use `SrunLauncher` to launch workers.
  - Set the walltime (e.g., "00:30:00").

Bridges (PSC):
^^^^^^^^^^^^^^

- **Load Modules**: Load the necessary modules for Parsl and your Python environment. This typically involves commands like `module load python/3.7.0` and `module load parsl`.
- **Configuration**: Create a `Config` object.
  - Use `HighThroughputExecutor` to run many tasks in parallel.
  - Set address using `address_by_interface('ens3f0')` (assuming Parsl runs on a login node).
  - Set `max_workers_per_node` to 1.
  - Use `SlurmProvider` to connect to the scheduler.
  - Specify your partition name (e.g., "RM-small") and account.
  - Set the walltime (e.g., "00:10:00").
  - Use `SrunLauncher` to launch workers.
  - Optionally, set `cmd_timeout` to 120 seconds for slower scheduler responses.

Expanse (SDSC):
^^^^^^^^^^^^^^^

- **Configuration**: Create a `Config` object.
  - Use the `HighThroughputExecutor`.
  - Set `max_workers_per_node` to 32.
  - Use `SlurmProvider` to connect to the scheduler.
  - Specify the partition as "compute" and your allocation account.
  - Use `SrunLauncher` to launch workers.
  - Set the walltime (e.g., "01:00:00").
- Include any necessary worker initialization commands (e.g., module loads).

Comet (SDSC):
^^^^^^^^^^^^^

Configuration: Comet uses the same configuration structure as Expanse (SDSC). Follow the steps for Expanse, adjusting the account and scheduler_options as needed for Comet.

ASPIRE 1 (NSCC):
^^^^^^^^^^^^^^^^

- **Load Modules**: Load the required modules for Parsl and your Python environment (e.g., `module load conda` followed by `conda activate parsl_mpi_py310`).
- **Configuration**: Create a `Config` object.
  - Use the `HighThroughputExecutor`.
  - Set `max_workers_per_node` to 4.
  - Set address using `address_by_interface('ib0')`.
  - Use `PBSProProvider` to connect to the scheduler.
  - Use `MpiRunLauncher` to launch workers.
  - Set the walltime (e.g., "24:00:00"), nodes_per_block (e.g., 3), and your account information.
  - Optionally, enable monitoring using `MonitoringHub`.

TOSS3 (LLNL):
^^^^^^^^^^^^^

- **Configuration**: Create a `Config` object.
  - Use the `FluxExecutor` for running Parsl apps as Flux jobs.
  - Use `SlurmProvider` to connect to the scheduler.
  - Specify your partition (e.g., "pbatch") and account.
  - Use `SrunLauncher` with `overrides="--mpibind=off"` to launch workers.
  - Set the walltime (e.g., "00:30:00").
  - Include any necessary worker initialization commands.

Important Considerations:
^^^^^^^^^^^^^^^^^^^^^^^^^

These configurations are aimed at showing simple examples. Refer to the official Parsl documentation and your supercomputer's user guide for detailed and up-to-date instructions.

- Always replace placeholders like `<YOUR_ALLOCATION>` with your actual account information.
- Adjust parameters like `nodes_per_block`, `max_workers_per_node`, and `walltime` based on your specific needs and the policies of your supercomputing center.
- If any of the instructions above fail, please do not hesitate to reach out to the Parsl community for help.
