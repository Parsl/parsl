The High-Throughput Executor
============================

The :class:`~parsl.executors.HighThroughputExecutor` is the standard Executor provided with Parsl.
It supports several advanced configuration options


.. contents::
   :local:
   :depth: 1

.. _label-data:

Staging data files
------------------

Parsl apps can take and return data files. A file may be passed as an input
argument to an app, or returned from an app after execution. Parsl
provides support to automatically transfer (stage) files between
the main Parsl program, worker nodes, and external data storage systems.

Input files can be passed as regular arguments, or a list of them may be
specified in the special ``inputs`` keyword argument to an app invocation.

Inside an app, the ``filepath`` attribute of a `File` can be read to determine
where on the execution-side file system the input file has been placed.

Output `File` objects must also be passed in at app invocation, through the
outputs parameter. In this case, the `File` object specifies where Parsl
should place output after execution.

Inside an app, the ``filepath`` attribute of an output
`File` provides the path at which the corresponding output file should be
placed so that Parsl can find it after execution.

If the output from an app is to be used as the input to a subsequent app,
then a `DataFuture` that represents whether the output file has been created
must be extracted from the first app's AppFuture, and that must be passed
to the second app. This causes app
executions to be properly ordered, in the same way that passing AppFutures
to subsequent apps causes execution ordering based on an app returning.

In a Parsl program, file handling is split into two pieces: files are named in an
execution-location independent manner using :py:class:`~parsl.data_provider.files.File`
objects, and executors are configured to stage those files in to and out of
execution locations using instances of the :py:class:`~parsl.data_provider.staging.Staging`
interface.


Parsl files
+++++++++++

Parsl uses a custom :py:class:`~parsl.data_provider.files.File` to provide a
location-independent way of referencing and accessing files.
Parsl files are defined by specifying the URL *scheme* and a path to the file.
Thus a file may represent an absolute path on the submit-side file system
or a URL to an external file.

The scheme defines the protocol via which the file may be accessed.
Parsl supports the following schemes: file, ftp, http, https, and globus.
If no scheme is specified Parsl will default to the file scheme.

The following example shows creation of two files with different
schemes: a locally-accessible data.txt file and an HTTPS-accessible
README file.

.. code-block:: python

    File('file://home/parsl/data.txt')
    File('https://github.com/Parsl/parsl/blob/master/README.rst')


Parsl automatically translates the file's location relative to the
environment in which it is accessed (e.g., the Parsl program or an app).
The following example shows how a file can be accessed in the app
irrespective of where that app executes.

.. code-block:: python

    @python_app
    def print_file(inputs=()):
        with open(inputs[0].filepath, 'r') as inp:
            content = inp.read()
            return(content)

    # create an remote Parsl file
    f = File('https://github.com/Parsl/parsl/blob/master/README.rst')

    # call the print_file app with the Parsl file
    r = print_file(inputs=[f])
    r.result()

As described below, the method by which this files are transferred
depends on the scheme and the staging providers specified in the Parsl
configuration.

Staging providers
+++++++++++++++++

Parsl is able to transparently stage files between at-rest locations and
execution locations by specifying a list of
:py:class:`~parsl.data_provider.staging.Staging` instances for an executor.
These staging instances define how to transfer files in and out of an execution
location. This list should be supplied as the ``storage_access``
parameter to an executor when it is constructed.

Parsl includes several staging providers for moving files using the
schemes defined above. By default, Parsl executors are created with
three common staging providers:
the NoOpFileStaging provider for local and shared file systems
and the HTTP(S) and FTP staging providers for transferring
files to and from remote storage locations. The following
example shows how to explicitly set the default staging providers.

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.data_provider.data_manager import default_staging

    config = Config(
        executors=[
            HighThroughputExecutor(
                storage_access=default_staging,
                # equivalent to the following
                # storage_access=[NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPSeparateTaskStaging()],
            )
        ]
    )


Parsl further differentiates when staging occurs relative to
the app invocation that requires or produces files.
Staging either occurs with the executing task (*in-task staging*)
or as a separate task (*separate task staging*) before app execution.
In-task staging
uses a wrapper that is executed around the Parsl task and thus
occurs on the resource on which the task is executed. Separate
task staging inserts a new Parsl task in the graph and associates
a dependency between the staging task and the task that depends
on that file.  Separate task staging may occur on either the submit-side
(e.g., when using Globus) or on the execution-side (e.g., HTTPS, FTP).


NoOpFileStaging for Local/Shared File Systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The NoOpFileStaging provider assumes that files specified either
with a path or with the ``file`` URL scheme are available both
on the submit and execution side. This occurs, for example, when there is a
shared file system. In this case, files will not moved, and the
File object simply presents the same file path to the Parsl program
and any executing tasks.

Files defined as follows will be handled by the NoOpFileStaging provider.

.. code-block:: python

    File('file://home/parsl/data.txt')
    File('/home/parsl/data.txt')


The NoOpFileStaging provider is enabled by default on all
executors. It can be explicitly set as the only
staging provider as follows.

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.data_provider.file_noop import NoOpFileStaging

    config = Config(
        executors=[
            HighThroughputExecutor(
                storage_access=[NoOpFileStaging()]
            )
        ]
    )


FTP, HTTP, HTTPS: separate task staging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Files named with the ``ftp``, ``http`` or ``https`` URL scheme will be
staged in using HTTP GET or anonymous FTP commands. These commands
will be executed as a separate
Parsl task that will complete before the corresponding app
executes. These providers cannot be used to stage out output files.

The following example defines a file accessible on a remote FTP server.

.. code-block:: python

    File('ftp://www.iana.org/pub/mirror/rirstats/arin/ARIN-STATS-FORMAT-CHANGE.txt')

When such a file object is passed as an input to an app, Parsl will download the file to whatever location is selected for the app to execute.
The following example illustrates how the remote file is implicitly downloaded from an FTP server and then converted. Note that the app does not need to know the location of the downloaded file on the remote computer, as Parsl abstracts this translation.

.. code-block:: python

    @python_app
    def convert(inputs=(), outputs=()):
        with open(inputs[0].filepath, 'r') as inp:
            content = inp.read()
            with open(outputs[0].filepath, 'w') as out:
                out.write(content.upper())

    # create an remote Parsl file
    inp = File('ftp://www.iana.org/pub/mirror/rirstats/arin/ARIN-STATS-FORMAT-CHANGE.txt')

    # create a local Parsl file
    out = File('file:///tmp/ARIN-STATS-FORMAT-CHANGE.txt')

    # call the convert app with the Parsl file
    f = convert(inputs=[inp], outputs=[out])
    f.result()

HTTP and FTP separate task staging providers can be configured as follows.

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.data_provider.http import HTTPSeparateTaskStaging
    from parsl.data_provider.ftp import FTPSeparateTaskStaging

		config = Config(
        executors=[
            HighThroughputExecutor(
                storage_access=[HTTPSeparateTaskStaging(), FTPSeparateTaskStaging()]
            )
        ]
    )

FTP, HTTP, HTTPS: in-task staging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These staging providers are intended for use on executors that do not have
a file system shared between each executor node.

These providers will use the same HTTP GET/anonymous FTP as the separate
task staging providers described above, but will do so in a wrapper around
individual app invocations, which guarantees that they will stage files to
a file system visible to the app.

A downside of this staging approach is that the staging tasks are less visible
to Parsl, as they are not performed as separate Parsl tasks.

In-task staging providers can be configured as follows.

.. code-block:: python

    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.data_provider.http import HTTPInTaskStaging
    from parsl.data_provider.ftp import FTPInTaskStaging

    config = Config(
        executors=[
            HighThroughputExecutor(
                storage_access=[HTTPInTaskStaging(), FTPInTaskStaging()]
            )
        ]
    )


Globus
^^^^^^

The ``Globus`` staging provider is used to transfer files that can be accessed
using Globus. A guide to using Globus is available `here
<https://docs.globus.org/how-to/get-started/>`_).

A file using the Globus scheme must specify the UUID of the Globus
endpoint and a path to the file on the endpoint, for example:

.. code-block:: python

        File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

Note: a Globus endpoint's UUID can be found in the Globus `Manage Endpoints <https://app.globus.org/endpoints>`_ page.

There must also be a Globus endpoint available with access to a
execute-side file system, because Globus file transfers happen
between two Globus endpoints.

Globus Configuration
""""""""""""""""""""

In order to manage where files are staged, users must configure the default ``working_dir`` on a remote location. This information is specified in the :class:`~parsl.executors.base.ParslExecutor` via the ``working_dir`` parameter in the :class:`~parsl.config.Config` instance. For example:

.. code-block:: python

        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor

        config = Config(
            executors=[
                HighThroughputExecutor(
                    working_dir="/home/user/data"
                )
            ]
        )

Parsl requires knowledge of the Globus endpoint that is associated with an executor. This is done by specifying the ``endpoint_name`` (the UUID of the Globus endpoint that is associated with the system) in the configuration.

In some cases, for example when using a Globus `shared endpoint <https://www.globus.org/data-sharing>`_ or when a Globus endpoint is mounted on a supercomputer, the path seen by Globus is not the same as the local path seen by Parsl. In this case the configuration may optionally specify a mapping between the ``endpoint_path`` (the common root path seen in Globus), and the ``local_path`` (the common root path on the local file system), as in the following. In most cases, ``endpoint_path`` and ``local_path`` are the same and do not need to be specified.

.. code-block:: python

        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor
        from parsl.data_provider.globus import GlobusStaging
        from parsl.data_provider.data_manager import default_staging

        config = Config(
            executors=[
                HighThroughputExecutor(
                    working_dir="/home/user/parsl_script",
                    storage_access=default_staging + [GlobusStaging(
                        endpoint_uuid="7d2dc622-2edb-11e8-b8be-0ac6873fc732",
                        endpoint_path="/",
                        local_path="/home/user"
                    )]
                )
            ]
        )


Globus Authorization
""""""""""""""""""""

In order to transfer files with Globus, the user must first authenticate.
The first time that Globus is used with Parsl on a computer, the program
will prompt the user to follow an authentication and authorization
procedure involving a web browser. Users can authorize out of band by
running the parsl-globus-auth utility. This is useful, for example,
when running a Parsl program in a batch system where it will be unattended.

.. code-block:: bash

        $ parsl-globus-auth
        Parsl Globus command-line authorizer
        If authorization to Globus is necessary, the library will prompt you now.
        Otherwise it will do nothing
        Authorization complete

rsync
^^^^^

The ``rsync`` utility can be used to transfer files in the ``file`` scheme in configurations where
workers cannot access the submit-side file system directly, such as when executing
on an AWS EC2 instance or on a cluster without a shared file system.
However, the submit-side file system must be exposed using rsync.

rsync Configuration
"""""""""""""""""""

``rsync`` must be installed on both the submit and worker side. It can usually be installed
by using the operating system package manager: for example, by ``apt-get install rsync``.

An `RSyncStaging` option must then be added to the Parsl configuration file, as in the following.
The parameter to RSyncStaging should describe the prefix to be passed to each rsync
command to connect from workers to the submit-side host. This will often be the username
and public IP address of the submitting system.

.. code-block:: python

        from parsl.data_provider.rsync import RSyncStaging

        config = Config(
            executors=[
                HighThroughputExecutor(
                    storage_access=[HTTPInTaskStaging(), FTPInTaskStaging(), RSyncStaging("benc@" + public_ip)],
                    ...
            )
        )

rsync Authorization
"""""""""""""""""""

The rsync staging provider delegates all authentication and authorization to the
underlying ``rsync`` command. This command must be correctly authorized to connect back to
the submit-side system. The form of this authorization will depend on the systems in
question.

The following example installs an ssh key from the submit-side file system and turns off host key
checking, in the ``worker_init`` initialization of an EC2 instance. The ssh key must have
sufficient privileges to run ``rsync`` over ssh on the submit-side system.

.. code-block:: python

        with open("rsync-callback-ssh", "r") as f:
            private_key = f.read()

        ssh_init = """
        mkdir .ssh
        chmod go-rwx .ssh

        cat > .ssh/id_rsa <<EOF
        {private_key}
        EOF

        cat > .ssh/config <<EOF
        Host *
          StrictHostKeyChecking no
        EOF

        chmod go-rwx .ssh/id_rsa
        chmod go-rwx .ssh/config

        """.format(private_key=private_key)

        config = Config(
            executors=[
                HighThroughputExecutor(
                    storage_access=[HTTPInTaskStaging(), FTPInTaskStaging(), RSyncStaging("benc@" + public_ip)],
                    provider=AWSProvider(
                    ...
                    worker_init = ssh_init
                    ...
                    )

            )
        )


Resource pinning
----------------

Resource pinning reduces contention between multiple workers using the same CPU cores or accelerators.

Multi-Threaded Applications
+++++++++++++++++++++++++++

Workflows which launch multiple workers on a single node which perform multi-threaded tasks (e.g., NumPy, Tensorflow operations) may run into thread contention issues.
Each worker may try to use the same hardware threads, which leads to performance penalties.
Use the ``cpu_affinity`` feature of the :class:`~parsl.executors.HighThroughputExecutor` to assign workers to specific threads.  Users can pin threads to
workers either with a strategy method or an explicit list.

The strategy methods will auto assign all detected hardware threads to workers.
Allowed strategies that can be assigned to ``cpu_affinity`` are ``block``, ``block-reverse``, and ``alternating``.
The ``block`` method pins threads to workers in sequential order (ex: 4 threads are grouped (0, 1) and (2, 3) on two workers);
``block-reverse`` pins threads in reverse sequential order (ex: (3, 2) and (1, 0)); and ``alternating`` alternates threads among workers (ex: (0, 2) and (1, 3)).

Select the best blocking strategy for processor's cache hierarchy (choose ``alternating`` if in doubt) to ensure workers to not compete for cores.

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

It is possible to bind multiple/specific accelerators to each worker by specifying a list of comma separated strings
each specifying accelerators. In the context of binding to NVIDIA GPUs, this works by setting ``CUDA_VISIBLE_DEVICES``
on each worker to a specific string in the list supplied to ``available_accelerators``.

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

GPU Oversubscription
^^^^^^^^^^^^^^^^^^^^

For hardware that uses Nvidia devices, Parsl allows for the oversubscription of workers to GPUS.  This is intended to
make use of Nvidia's `Multi-Process Service (MPS) <https://docs.nvidia.com/deploy/mps/>`_ available on many of their
GPUs that allows users to run multiple concurrent processes on a single GPU.  The user needs to set in the
``worker_init`` commands to start MPS on every node in the block (this is machine dependent).  The
``available_accelerators`` option should then be set to the total number of GPU partitions run on a single node in the
block.  For example, for a node with 4 Nvidia GPUs, to create 8 workers per GPU, set ``available_accelerators=32``.
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
^^^^^^^^^^^^^^^^^^^^^^

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
