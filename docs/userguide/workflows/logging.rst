..
   The logging examples are from the "examples" in the advanced user guide.

Logging
=======

Parsl produces a rich collection of output logs by default.
Different parts of Parsl write to separate files
and all are collected in a directory named "runinfo" by default.

Log Directory Structure
-----------------------

.. code-block:: text

    runinfo/
    └── 000
        ├── htex_local
        │   ├── block-0
        │   │   └── ad5bf9a8d238
        │   │       ├── manager.log
        │   │       ├── worker_0.log
        │   │       ├── worker_1.log
        │   │       ├── worker_2.log
        │   │       └── worker_3.log
        │   └── interchange.log
        ├── parsl.log
        └── submit_scripts
            ├── parsl.htex_local.block-0.1735499186.6675148.sh
            ├── parsl.htex_local.block-0.1735499186.6675148.sh.ec
            ├── parsl.htex_local.block-0.1735499186.6675148.sh.err
            └── parsl.htex_local.block-0.1735499186.6675148.sh.out


The ``runinfo`` directory holds a unique subdirectory for each time Parsl is executed.

Components of Parsl run in separate processes
and each write to different files to avoid any need for synchronization.
The root directory for a run contains:

1. ``parsl.log``: The core log file from main Parsl processes
2. ``submit_scripts``: A collection of the scripts which were run to launch workers.
   These scripts may include batch submission files and the text printed to screen
   during the batch job.
3. Subdirectories for logs from each of the Executors used by Parsl (here: ``htex_local``).

Main Parsl Log
~~~~~~~~~~~~~~

The ``parsl.log`` file in a Parsl logging directory captures all Parsl-related activities
that occur on the Python process running the Parsl application.

The log should begin with a message indicating Parsl DataFlowKernel (DFK) is starting
and a readout of the configuration settings.
The logs will end with a status message that the DFK has cleaned up successfully.

.. code-block:: text

    1735499181.631377 2024-12-29 14:06:21 MainProcess- MainThread-   parsl.dataflow.dflow:95 __init__ INFO: Starting DataFlowKernel with config
    [ ... ]
    1735499187.851164 2024-12-29 14:06:27 MainProcess- MainThread-   parsl.dataflow.dflow:1265 cleanup INFO: DFK cleanup complete
    1735499187.851178 2024-12-29 14:06:27 MainProcess- MainThread-   parsl.process_loggers:27 wrapped DEBUG: Normal ending for cleanup on thread MainThread

As :ref:`exampled below <section-logging-tasks>`, the main Parsl log contains references for
status changes of blocks of workers and each task.

Submission Scripts
~~~~~~~~~~~~~~~~~~

The ``submit_script`` directory contains the input and outputs from the :ref:`execution providers <label-execution>`
used by a Parsl application.
The actual contents of the file vary between Parsl configurations,
but most providers will produce a shell script
used when requesting compute nodes and the outputs of the shell scripts.

Executor Logs
~~~~~~~~~~~~~

Different Executors in the Parsl application write log files to their own directories.
The types of logs vary depending on the choice of Executor.
This section describes the output structure of a common configuration:
:ref:`HighThroughputExecutor <label-htex>` using a batch scheduler.

The log file at the root of the directory, ``interchange.log``, is from a process
which distributes task across the workers used by Parsl.
The interchange reports the connection between the main Parsl process
and managers which register to it.

.. code-block:: text

    2024-12-29 14:06:21.965 interchange:115 MainProcess(52425) MainThread __init__ [INFO] Attempting connection to client at 127.0.0.1 on ports: 55059,55181,55929
    2024-12-29 14:06:21.966 interchange:127 MainProcess(52425) MainThread __init__ [INFO] Connected to client
    2024-12-29 14:06:21.966 interchange:160 MainProcess(52425) MainThread __init__ [INFO] Bound to ports 54160,54943 for incoming worker connections
    [ ... ]
    2024-12-29 14:06:21.966 interchange:209 MainProcess(52425) Interchange-Task-Puller task_puller [INFO] Starting
    2024-12-29 14:06:27.316 interchange:411 MainProcess(52425) MainThread process_task_outgoing_incoming [INFO] Adding manager: b'ad5bf9a8d238' to ready queue

Each manager writes logs to a unique subdirectory named by the block it belongs
to and the name assigned to it.
The structure shown above includes one manager directory: ``htex_local/block‑0/ad5bf9a8d238``.
The directory contains two types of files:

1. A single manager log reporting connections to the other Parsl components,
2. Many worker logs reporting which CPUs or accelerators they use, and which tasks they have processed.

.. _section-logging-tasks:

Common Debugging Tasks
----------------------

.. note::

    Consider the :ref:`monitoring module <label-monitoring>` for developing
    components that display the current status of a workflow.

This section describes how to use the logs to check correctness
and locate errors in common operations.

Launching Blocks
~~~~~~~~~~~~~~~~

Parsl groups compute nodes into :ref:`blocks <label-elasticity>` that are acquired
and released as compute requirements change.
The process for launching a block is recorded in a sequence:

1. A request for "scaling out" in ``parsl.log``.

   Log messages starting with ``[Scaling executor <name>]`` record the process for
   deciding whether to request another block and status messages while acquiring
   a new block of workers.

   Typical log lines for requesting a block start with a request
   and end with a job ID from the :ref:`provider <label-execution>`:

   .. code-block:: text

      [...] parsl.jobs.strategy:195 _general_strategy DEBUG: [Scaling executor htex_local] Strategizing for executor
      [...] parsl.jobs.strategy:198 _general_strategy DEBUG: [Scaling executor htex_local] Scaling out 1 initial blocks
      [...] parsl.executors.status_handling:204 scale_out_facade INFO: [Scaling executor htex_local] Scaling out by 1 blocks
      [...] parsl.executors.status_handling:207 scale_out_facade INFO: [Scaling executor htex_local] Allocated block ID 0
      [...]
      [...] parsl.executors.status_handling:268 _launch_block DEBUG: [Scaling executor htex_local] Launched block 0 with job ID 20076


   The last message, "Launched block <N> with job ID <>," indicates
   a request was successfully made to the execution provider.

   The job ID is often the same used by the scheduling system underlying the Provider.
   Use standard tools (e.g., ``squeue`` for Slurm) to monitor whether the requested nodes are available.

2. The results from requesting nodes are found in the ``submit_scripts`` directory.

   Use the output files and the job ID from the previous step to determine whether
   the block was launched properly.
   The file names contain the name of the executor and ID associated with the block of workers.

   Common errors that can be found in the output files include:

   - Parsl worker services not found because the :ref:`Python environment was not activated <label-worker-init>`
   - The launcher (e.g., ``srun``) fails to work because of incorrect options.
   - Workers fail to connect to the host process :ref:`due to network configuration problems <label-networking>`

Tracing Compute Nodes
~~~~~~~~~~~~~~~~~~~~~

.. note::

    This sections describes the procedure for Parsl using
    :ref:`the high-throughput executor <label-htex>`
    and may not be applicable to other executors.

Use logs specific to an Executor to trace whether the workers on the compute nodes
are starting, detecting hardware properly, and connecting to the host process.

Each node in a compute block writes to a unique subdirectory within
the directory of its associated executor and block.
For example, a compute node in the first block of the "htex_local" executor
is found in ``htex_local/block-0``.

Use the logs written by processes on the compute node to ensure...

- *Connectivity* to the host process. The manager log should declare that it connected
  and identify which network it is using by printing a log line similar to

  .. code-block:: text

     2025-12-29 09:22:12.683 parsl:322 20081 Interchange-Communicator [INFO]  Successfully connected to interchange via URL: tcp://192.168.1.71:54495

  The ``interchange.log`` in the root directory for the executor will include a matching line
  after acknowledging the connection and registering the workers as ready for tasks.

  .. code-block:: text

     2025-12-29 09:22:12.779 interchange:438 MainProcess(20069) MainThread process_manager_socket_message [INFO] Registered manager b'c4b55da1a90f' (py3.11, 1.3.0-dev) and added to ready queue

- *Resource Pinning* for the workers.
  Each worker log begins with a message that it has started then,
  if affinity is enabled,
  a list of hardware (CPU and/or GPU) that it will use for tasks.

  .. code-block:: text

     2025-12-29 09:22:13.110 worker_log:658 20100 MainThread [INFO]  Worker 7 started
     2025-12-29 09:22:13.112 worker_log:715 20100 MainThread [INFO]  Set worker CPU affinity to [7]


Tracking Task Status
~~~~~~~~~~~~~~~~~~~~

The ``parsl.log`` provides information about a task across its lifespan.

1. The task is submitted. Parsl logs which executor it is assigned to
   and the name of the App it is running, and any dependencies.

  .. code-block:: text

     1763128191.056197 2025-11-14 05:49:51 MainProcess- ThreadPoolExecutor-2_0- parsl.dataflow.dflow:973 submit DEBUG: Task 316 will be sent to executor _parsl_internal
     1763128191.058182 2025-11-14 05:49:51 MainProcess- ThreadPoolExecutor-2_0- parsl.dataflow.dflow:1052 submit INFO: Task 316 submitted for App launch_relaxations, waiting on task 315
     1763128191.058387 2025-11-14 05:49:51 MainProcess- ThreadPoolExecutor-2_0- parsl.dataflow.dflow:1058 submit DEBUG: Task 316 set to pending state with AppFuture: <AppFuture at 0x7fb6ce3da510 state=pending>
     1763128191.058833 2025-11-14 05:49:51 MainProcess- Task-Launch_0- parsl.dataflow.dflow:629 _launch_if_ready_async DEBUG: Task 316 has outstanding dependencies, so launch_if_ready skipping

  The above task is a :ref:`join app <label-joinapp>` named ``launch_relaxations``,
  and will run on the internal executor for Parsl (``_parsl_internal``).
  It is dependent on task 315, so will not start immediately.

2. The task is launched after all dependencies are done.

  .. code-block:: text

     1763130591.542695 2025-11-14 06:29:51 MainProcess- Task-Launch_0- parsl.dataflow.memoization:269 check_memo DEBUG: Task 316 will not be memoized
     1763130591.543230 2025-11-14 06:29:51 MainProcess- Task-Launch_0- parsl.dataflow.dflow:739 launch_task INFO: Parsl task 316 try 0 launched on executor _parsl_internal

  A single task may be :ref:`tried multiple times <label-retry>` and the attempt number is recorded in the logs as well.

3. The result is received. If successful, Parsl will print a single completion message.

  .. code-block:: text

    1763147214.980597 2025-11-14 11:06:54 MainProcess- HTEX-Result-Queue-Thread- parsl.dataflow.dflow:539 _complete_task_result INFO: Task 316 completed (joining -> exec_done)

  If unsuccessful, the task could be restarted and relaunched

  .. code-block:: text

     1763129113.044855 2025-11-14 06:05:13 MainProcess- HTEX-Result-Queue-Thread- parsl.dataflow.dflow:331 handle_exec_update INFO: Task 204 try 0 failed with exception of type ValueError
     1763129113.045055 2025-11-14 06:05:13 MainProcess- HTEX-Result-Queue-Thread- parsl.dataflow.dflow:371 handle_exec_update INFO: Task 204 marked for retry
     [...]
     1763129113.050172 2025-11-14 06:05:13 MainProcess- Task-Launch_0-140414082344704 parsl.dataflow.dflow:734 launch_task INFO: Parsl task 204 try 1 launched on executor xtb with executor id 445

  or failed and the exception both recorded in the logs and
  :ref:`exposed to the user as an exception in the app Future <label-exceptions>`.

  .. code-block:: text

     1763137697.445044 2025-11-14 08:28:17 MainProcess- HTEX-Result-Queue-Thread- parsl.app.errors:110 reraise DEBUG: Reraising exception of type <class 'ValueError'>
     1763137697.445302 2025-11-14 08:28:17 MainProcess- HTEX-Result-Queue-Thread- parsl.dataflow.dflow:331 handle_exec_update INFO: Task 204 try 1 failed with exception of type ValueError
     1763137697.445468 2025-11-14 08:28:17 MainProcess- HTEX-Result-Queue-Thread- parsl.dataflow.dflow:374 handle_exec_update ERROR: Task 204 failed after 1 retry attempts
     Traceback (most recent call last):
       File "/global/cfs/env/lib/python3.12/site-packages/parsl/dataflow/dflow.py", line 328, in handle_exec_update
     [...]
         return umr_minimum(a, axis, None, out, keepdims, initial, where)
         ^^^^^^^^^^^^^^^^^
     ValueError: zero-size array to reduction operation minimum which has no identity
