8. Execution Environment
========================

The execution environment in Parsl refers to the resources and settings that affect how your Parsl apps run. This includes the memory available to each app, the file system where files are stored and accessed, and any external services that the apps might need to interact with.

Memory Environment
------------------

Each Parsl app runs in its own memory space. This means that the variables and objects created within one app are not directly accessible to other apps. This isolation helps prevent conflicts and ensures that each app can run independently.

However, there is one exception: when you use the `ThreadPoolExecutor`, all apps run within the same Python process and therefore share the same memory space. This can be useful for sharing large objects between apps, but it also means that you need to be careful to avoid race conditions and other concurrency issues.

File System Environment
-----------------------

Parsl apps can access files on local or remote filesystems. When an app is executed on a remote worker, Parsl automatically transfers any input files to the worker's file system and transfers any output files back to the submitter's file system.

The specific file system environment that an app has access to depends on where the app is executed. If two tasks run on nodes that share a file system, then those tasks share a file system environment. However, if the tasks run on nodes that do not share a file system, they will not be able to access each other's files directly.

Service Environment
-------------------

The service environment refers to any external services that your Parsl apps might need to interact with, such as databases, web services, or message queues. These services are typically accessed over a network, and they are available to all Parsl apps, regardless of where they are executed.

Summary of Environments
-----------------------

.. list-table:: Summary of Environments
   :header-rows: 1

   * - Environment
     - Shared with Parent/Other Tasks (ThreadPoolExecutor)
     - Shared with Parent/Other Tasks (Other Executors)
   * - Memory
     - Yes
     - No
   * - File System
     - Yes
     - Depends on location 
   * - Service (Network)
     - N/A
     - Yes

Interactive Tutorial: Setting Up Your Execution Environment
-----------------------------------------------------------

The Parsl documentation provides interactive tutorials that guide you through the process of setting up your execution environment. These tutorials cover topics such as:

- Choosing the right executor for your needs
- Configuring Parsl to run on different types of resources (e.g., local machine, cluster, cloud)
- Managing data dependencies between tasks
- Handling errors and failures

Two Practical Examples
----------------------

Scenario 1: Data Analysis on a Personal Computer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Imagine you are a data scientist working on a research project. You have a large dataset that you need to analyze, and you want to speed up the process by using Parsl to run your analysis code in parallel on your multi-core laptop.

1. **Choose the ThreadPoolExecutor**: Since you're working on a single machine, the `ThreadPoolExecutor` is the most suitable choice. It allows you to utilize multiple threads of your CPU to run tasks concurrently.

2. **Create a Config object**:

   .. code-block:: python

      from parsl.config import Config
      from parsl.executors import ThreadPoolExecutor

      config = Config(
          executors=[ThreadPoolExecutor(max_threads=8)]  # Use 8 threads (adjust based on your CPU)
      )

3. **Load the configuration**:

   .. code-block:: python

      import parsl

      parsl.load(config)

4. **Write and run your Parsl script**: Define your data analysis tasks as Parsl apps and call them in your script. Parsl will automatically execute these tasks in parallel on the available threads.

Scenario 2: Large-Scale Simulation on a Supercomputer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, imagine you're a computational scientist running a complex simulation that requires a large amount of computing power. You have access to a supercomputer like Summit at Oak Ridge National Laboratory, and you want to use Parsl to distribute your simulation across multiple nodes of the supercomputer.

1. **Load Modules**: Load the necessary modules for Parsl and your Python environment. This typically involves commands like `module load python` and `module load parsl`.

2. **Choose the HighThroughputExecutor and LSFProvider**: The `HighThroughputExecutor` is designed for running large-scale parallel workflows on clusters and supercomputers. The `LSFProvider` is used to interact with the LSF job scheduler on Summit.

3. **Create a Config object**:

   .. code-block:: python

      from parsl.config import Config
      from parsl.executors import HighThroughputExecutor
      from parsl.providers import LSFProvider
      from parsl.launchers import JsrunLauncher

      config = Config(
          executors=[
              HighThroughputExecutor(
                  label="Summit_HTEX",
                  working_dir="/path/to/your/shared/filesystem",  # Use a shared file system
                  provider=LSFProvider(
                      launcher=JsrunLauncher(),
                      walltime="02:00:00",  # Adjust walltime as needed
                      nodes_per_block=2,  # Request 2 nodes per block
                      init_blocks=1,
                      max_blocks=10,  # Adjust based on your needs
                      worker_init="module load ...",  # Load any necessary modules
                      project="YOUR_PROJECT_ALLOCATION",
                  ),
              )
          ]
      )

4. **Load the configuration**:

   .. code-block:: python

      parsl.load(config)

5. **Write and run your Parsl script**: Define your simulation tasks as Parsl apps and call them in your script. Parsl will submit these tasks to the LSF scheduler, which will then distribute them across the nodes of the supercomputer.
