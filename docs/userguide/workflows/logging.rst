Logging
=======

Parsl produces a rich collection of output logs by default.
Different parts of Parsl write to separate files
and all are collected in a directory named "runinfo" by default.

Directory Structure
-------------------

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

The root folder of the directory for a single run contains:

1. ``parsl.log``: The core log file from main Parsl processes
2. ``submit_scripts``: A collection of the scripts which were run to launch workers.
   These scripts may include batch submission files and the text printed to screen.
3. Subdirectories for logs from each of the Executors used by Parsl (here: ``htex_local``).

   Each Executor often contain an ``interchange.log`` for the process
   used to communicate with managers and workers running on compute nodes.

   The managers and workers have separate logs grouped by the block number (here: ``block-0``)
   and the manager name (here: ``ad5bf9a8d238``).

Log File Contents
-----------------

Components of Parsl run in separate processes
and each write to different files to avoid any need for synchronization.
We describe here the components used in a common deployment model for Parsl:
a ``HighThroughputExecutor`` deployed onto nodes provided by a batch scheduler.

Main Parsl Log
~~~~~~~~~~~~~~

The ``parsl.log`` file in a Parsl logging directory captures all Parsl-related activities
that occur on the process running the Parsl application.

The log should begin with a message indicating Parsl DataFlowKernel (DFK) is starting
and a readout of the configuration settings.
The logs will end with a status message that the DFK has cleaned up resources successfully

.. code-block:: text

    1735499181.631377 2024-12-29 14:06:21 MainProcess- MainThread-   parsl.dataflow.dflow:95 __init__ INFO: Starting DataFlowKernel with config
    [ ... ]
    1735499187.851164 2024-12-29 14:06:27 MainProcess- MainThread-   parsl.dataflow.dflow:1265 cleanup INFO: DFK cleanup complete
    1735499187.851178 2024-12-29 14:06:27 MainProcess- MainThread-   parsl.process_loggers:27 wrapped DEBUG: Normal ending for cleanup on thread MainThread

[ TBD: Describe that it also reports when task are started/received, and management of blocks ]

Interchange Logs
~~~~~~~~~~~~~~~~

[ TBD ]

Manager Logs
~~~~~~~~~~~~

[ TBD ]

Worker Logs
~~~~~~~~~~~

Worker logs report only the resources they are pinned to on startup,
then when the receive or complete tasks.

Common Logging Tasks
--------------------

Parsl Workers

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

[ TBD: Describe how to access the number of active tasks/blocks/workers]
