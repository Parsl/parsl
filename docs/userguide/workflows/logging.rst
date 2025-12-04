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
The logs will end with a status message that the DFK has cleaned up resources successfully

.. code-block:: text

    1735499181.631377 2024-12-29 14:06:21 MainProcess- MainThread-   parsl.dataflow.dflow:95 __init__ INFO: Starting DataFlowKernel with config
    [ ... ]
    1735499187.851164 2024-12-29 14:06:27 MainProcess- MainThread-   parsl.dataflow.dflow:1265 cleanup INFO: DFK cleanup complete
    1735499187.851178 2024-12-29 14:06:27 MainProcess- MainThread-   parsl.process_loggers:27 wrapped DEBUG: Normal ending for cleanup on thread MainThread

As :ref:`exampled below <section-logging-tasks>`, the main Parsl log contains references for
status changes of blocks of workers and each task.

Submission Scripts
~~~~~~~~~~~~~~~~~~

The ``submit_script`` directory contains the input and outputs from the :ref:`resource providers <label-execution>`
used by a Parsl application.
The actual contents of the file vary, but most providers will produce a shell script
used when requesting resources and the outputs of the shell scripts

Executor Logs
~~~~~~~~~~~~~

The remaining directories are used by the different Executors in the Parsl application.
The types of logs vary depending on the choice of Executor.
This section describes the output structure of a common configuration:
:ref:`HighThroughputExecutor <label-htex>` deployed onto nodes provided by a batch scheduler.

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
2. Many worker logs reporting the resources they use, and which tasks they have processed.

.. _section-logging-tasks:

Common Logging Tasks
--------------------

.. note::

    The :ref:`monitoring module<label-monitoring>` collects logging information into a single database
    and includes visualization tools.
    Consider using Monitoring as a basis for routine logging efforts.

Many common logging tasks require processing information from multiple files
as a single operation may coordinated across multiple processes.
This section describes how to use the logs to check correctness
and locate errors in common operations.

Tracking Manager Status
~~~~~~~~~~~~~~~~~~~~~~~

The log should also contain messages about blocks of workers being requested,
starting, and being reported closed.

.. code-block:: text

    TO BE FOUND

Tracking Task Status
~~~~~~~~~~~~~~~~~~~~

The tasks being submitted, passing, or failing will also be visible in the logs.

.. code-block:: text

    TO BE FOUND

Monitoring Workflow Status
~~~~~~~~~~~~~~~~~~~~~~~~~~

[ TBD: Describe how to access the number of active tasks/blocks/workers ]
