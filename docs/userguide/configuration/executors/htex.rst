The High-Throughput Executor
============================

The :class:`~parsl.executors.HighThroughputExecutor` (HTEx) is the standard Executor provided with Parsl.
The following sections detail the most-used configuration options of HTEx.

.. contents::
   :local:
   :depth: 1

Defining Workers Per Node
-------------------------

HTEx determines how many workers to run on each node individually.
The number of nodes is the minimum of the ``max_workers_per_node``,
the number of workers determined from ``cores_per_worker``,
and the number of workers defined by ``mem_per_worker``.

Resource pinning
----------------

Resource pinning reduces contention between multiple workers using the same CPU cores or accelerators.

Multi-Threaded Applications
+++++++++++++++++++++++++++

Workflows which launch multiple workers on a single node which perform multi-threaded tasks (e.g., NumPy, Tensorflow operations) may run into thread contention issues.
Each worker may try to use the same hardware threads, which leads to performance penalties.
Use ``cpu_affinity`` to assign workers to specific threads either by 
defining an allocation strategy method or an explicit list.

The strategy methods will auto assign all detected hardware threads to workers.
Allowed strategies are ``block``, ``block-reverse``, and ``alternating``.
The ``block`` method pins threads to workers in sequential order (ex: 4 threads are grouped (0, 1) and (2, 3) on two workers);
``block-reverse`` pins threads in reverse sequential order (ex: (3, 2) and (1, 0)); and ``alternating`` alternates threads among workers (ex: (0, 2) and (1, 3)).

Select the best blocking strategy based on the node's CPU cache hierarchy (query the cache hierarchy using ``lscpu``).

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                cpu_affinity='alternating',
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

Users can also use ``cpu_affinity`` to assign explicitly threads to workers with a string that has the format of
``cpu_affinity="list:<worker1_threads>:<worker2_threads>:<worker3_threads>"``.

Each worker's threads can be specified as a comma separated list or a hyphenated range:
``thread1,thread2,thread3``
or
``thread_start-thread_end``.

An example for 12 workers on a node with 208 threads is:

.. code-block:: python

    cpu_affinity="list:0-7,104-111:8-15,112-119:16-23,120-127:24-31,128-135:32-39,136-143:40-47,144-151:52-59,156-163:60-67,164-171:68-75,172-179:76-83,180-187:84-91,188-195:92-99,196-203"

This example assigns 16 threads each to 12 workers. Note that in this example there are threads that are skipped.
If a thread is not explicitly assigned to a worker, it will be left idle.
The number of thread "ranks" (colon separated thread lists/ranges) must match the total number of workers on the node; otherwise an exception will be raised.

Thread affinity is accomplished in two ways.
Each worker first sets the affinity for the Python process using `the affinity mask <https://docs.python.org/3/library/os.html#os.sched_setaffinity>`_,
which may not be available on all operating systems.
It then sets environment variables to control
`OpenMP thread affinity <https://hpc-tutorials.llnl.gov/openmp/ProcessThreadAffinity.pdf>`_
so that any subprocesses launched by a worker which use OpenMP know which processors are valid.
These include ``OMP_NUM_THREADS``, ``GOMP_COMP_AFFINITY``, and ``KMP_THREAD_AFFINITY``.

Accelerators
++++++++++++

Many modern clusters provide multiple accelerators per compute node, yet many applications are best suited to using a
single accelerator per task. Parsl supports pinning each worker to different accelerators using
``available_accelerators`` option of the :class:`~parsl.executors.HighThroughputExecutor`. Provide either the number of
executors (Parsl will assume they are named in integers starting from zero) or a list of the names of the accelerators
available on the node. Parsl will limit the number of workers it launches to the number of accelerators specified,
in other words, you cannot have more workers per node than there are accelerators. By default, Parsl will launch
as many workers as the accelerators specified via ``available_accelerators``.

.. code-block:: python

    local_config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                available_accelerators=2,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy='none',
    )

It is possible to bind multiple/specific accelerators to each worker by specifying a list of comma separated strings,
each specifying accelerators.

Here's an example:

.. code-block:: python

    # The following config is trimmed for clarity
    local_config = Config(
        executors=[
            HighThroughputExecutor(
                # Starts 2 workers per node, each bound to 2 GPUs
                available_accelerators=["0,1", "2,3"],

                # Start a single worker bound to all 4 GPUs
                # available_accelerators=["0,1,2,3"]
            )
        ],
    )

Binding is achieved by setting ``CUDA_VISIBLE_DEVICES`` (specific to NVIDIA GPUs), 
``ROCR_VISIBLE_DEVICES`` (AMD GPUs),
and ``ZE_AFFINITY_MASK`` (Intel GPUs) to the appropriate accelerator names.

GPU Oversubscription
^^^^^^^^^^^^^^^^^^^^

For hardware that uses NVIDIA devices, Parsl allows for the oversubscription of workers to GPUs.  This is intended to
make use of NVIDIA's `Multi-Process Service (MPS) <https://docs.nvidia.com/deploy/mps/>`_ available on many of their
GPUs that allows users to run multiple concurrent processes on a single GPU.  The user needs to set the
``worker_init`` command of the Provider to start MPS on every node in the block (this is machine dependent).  The
``available_accelerators`` option should then be set to the total number of GPU partitions run on a single node in the
block.  For example, for a node with 4 NVIDIA GPUs, to create 8 workers per GPU, set ``available_accelerators=32``.
GPUs will be assigned to workers in ascending order in contiguous blocks.  In the example, workers 0-7 will be placed
on GPU 0, workers 8-15 on GPU 1, workers 16-23 on GPU 2, and workers 24-31 on GPU 3.

Encryption
----------

Users can encrypt traffic between the Parsl DFK and ``HighThroughputExecutor`` instances by setting its ``encrypted``
initialization argument to ``True``.

For example,

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor

    config = Config(
        executors=[
            HighThroughputExecutor(
                encrypted=True
            )
        ]
    )

Under the hood, we use `CurveZMQ <http://curvezmq.org/>`_ to encrypt all communication channels
between the executor and related nodes.

Encryption performance
++++++++++++++++++++++

CurveZMQ depends on `libzmq <https://github.com/zeromq/libzmq>`_ and  `libsodium <https://github.com/jedisct1/libsodium>`_,
which `pyzmq <https://github.com/zeromq/pyzmq>`_ (a Parsl dependency) includes as part of its
installation via ``pip``. This installation path should work on most systems, but users have
reported significant performance degradation as a result.

If you experience a significant performance hit after enabling encryption, we recommend installing
``pyzmq`` with conda:

.. code-block:: bash

    conda install conda-forge::pyzmq

Alternatively, you can `install libsodium <https://doc.libsodium.org/installation>`_, then
`install libzmq <https://zeromq.org/download/>`_, then build ``pyzmq`` from source:

.. code-block:: bash

    pip3 install parsl --no-binary pyzmq
