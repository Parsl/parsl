.. _label-data:

Passing Python objects
======================

Parsl apps can communicate via standard Python function parameter passing
and return statements. The following example shows how a Python string
can be passed to, and returned from, a Parsl app.

.. code-block:: python

    @python_app
    def example(name):
        return 'hello {0}'.format(name)

    r = example('bob')
    print(r.result())

Parsl uses the dill and pickle libraries to serialize Python objects
into a sequence of bytes that can be passed over a network from the submitting
machine to executing workers.

Thus, Parsl apps can receive and return standard Python data types
such as booleans, integers, tuples, lists, and dictionaries. However, not
all objects can be serialized with these methods (e.g., closures, generators,
and system objects), and so those objects cannot be used with all executors.

Parsl will raise a `SerializationError` if it encounters an object that it cannot
serialize. This applies to objects passed as arguments to an app, as well as objects
returned from an app. See :ref:`label_serialization_error`.


Staging data files
==================

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
-----------

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

Dynamic File Lists
------------------

When Parsl constructs the DAG for a workflow, it needs to know the input and
output files for each app. This works well when the number of files is known
in advance, and the lists of files are fully constructed before the workflow
is submitted. However, in some cases, the number of files produced by an app
is not know until the app executes. In these cases, Parsl provides a mechanism
called :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList` to dynamically
construct the list of files. This class inherits from both the Python
`Future <https://en.wikipedia.org/wiki/Futures_and_promises>`_ and
`list <https://docs.python.org/3/tutorial/datastructures.html>`_ classes, but
differs from traditional lists in the a few notable ways:

1. It can only contain :py:class:`~parsl.data_provider.files.File` and :py:class:`~parsl.app.futures.DataFuture` objects.
2. It is truly dynamic, meaning that you can access an item in the list which is beyond the current length of the list. The list will grow to accomodate the request and return a future that will be resolved when the item is available. (see `Dynamic Scaling`_ for more details)


Dynamic Scaling
^^^^^^^^^^^^^^^

The :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList` is designed
to be used in cases where the number of files produced by an app is not known
until the app executes. In these cases, the list can be accessed beyond its
current length, and the list will grow to accomodate the request. In addition
to the traditional methods of expanding a list (via ``append`` or ``extend``,
the :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList` will grow
when assigning a member by index to a position beyond its current length or
when accessing a member by index beyond the current length. In each of these
cases the list is expanded and filled with
:py:class:`~parsl.data_provider.dynamic_files.DynamicFileList.DynamicFile`
objects that are initialized with ``None``. When the file is available, the
``None`` is replaced with the actual file. This behavior allows for the
following code to work:

.. code-block:: python

    from parsl.app.app import python_app
    from parsl.data_provider.dynamic_files import DynamicFileList
    from parsl.data_provider.files import File


    dfl = DynamicFileList()                    # dfl = []
                                               #          A       B       C
    dfl[2] = File('file://path/to/file')       # dfl = [File(), File(), File('file://path/to/file')]

    myfile = dfl[0]                            # myfile is a reference to the File() labeled A in dfl

    dfl[0] = File('file://path/to/file.2')     # dfl = [File('file://path/to/file.2'), File(),
                                               #        File('file://path/to/file')]
                                               # but myfile still points to the File() labeled A

    fh = open(myfile, 'r')                     # works

    dfl[3] = File('file://path/to/file.3')     # dfl = [File('file://path/to/file.2'), File(),
                                               #        File('file://path/to/file'), File('file://path/to/file.3')]

    myfile2 = dfl[1]                           # myfile2 is a reference to the File() labeled B in dfl

    dfl[1] = File('file://path/to/file.4')     # dfl = [File('file://path/to/file.2'), File('file://path/to/file.4'),
                                               #        File('file://path/to/file'), File('file://path/to/file.3')]
                                               # myfile2 still points to the File() labeled B

    fh2 = open(myfile2, 'r')                   # works

    dfl[5] = File('file://path/to/file.5')     # dfl = [File('file://path/to/file.2'), File('file://path/to/file.4'),
                                               #        File('file://path/to/file'), File('file://path/to/file.3'),
                                               #        None, File('file://path/to/file.5')]

    myfile3 = dfl[4]                           # myfile3 is a reference to the DynamicFile() labeled None in dfl

    dfl[4] = File('file://path/to/file.6')     # dfl = [File('file://path/to/file.2'), File('file://path/to

Specifically, the behavior will allow Parsl Apps to use as input files which
do not exist at the time the App is created, but are produced by other Apps
in the workflow, even when the number of output files is not know beforehand.
For example:

.. code-block:: python

    from parsl.app.app import python_app
    from parsl.data_provider.dynamic_files import DynamicFileList
    from parsl.data_provider.files import File

    @python_app
    def produce(outputs=[]):
        from random import random
        from parsl.data_provider.files import File

        def analyze(i):
            f = File(f'file://path/to/file{i}.log}')
            with open(f.filepath, 'w') as out:
                # do some kind of anaylsis
                # write out log messages
            return f

        count = int(random() * 10)
        fl = File(f'file://path/to/master.log')
        outputs.append(fl)
        with open(fl.filepath, 'w') as log:
            log.write(f'Producing {count} files\n')
            for i in range(count):
                log.write(f"Running analysis {i}\n")
                outputs.append(analyze(i))

    @python_app
    def consume(inputs=[]):
        for i in range(len(inputs)):
            with open(inputs[i].filepath, 'r') as inp:
                content = inp.read()
                # do something with the log content

    dfl = DynamicFileList()                    # dfl = []

    f = produce(dfl)                           # dfl starts out empty, but is populated by produce

    r = consume(inputs=[dfl[1:]])              # consume waits to run until files are available,
                                               #    then does its work on the selected log files
    r.result()

Without a :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList`,
the above code would not work because when the DAG is constructed, the
``inputs`` to ``consume`` is a slice of an empty list. However, the
:py:class:`~parsl.data_provider.dynamic_files.DynamicFileList` will populate
the ``dfl`` list with two empty :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList.DynamicFile`
objects (indicies 0 and 1) and then fill them in with the actual files produced by
``produce`` when they are available. The ``consume`` app will then wait
until the files are available before running.

Dynamic Files
^^^^^^^^^^^^^

Technically the :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList`
holds instances of :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList.DynamicFile`,
a `Future <https://en.wikipedia.org/wiki/Futures_and_promises>`_, which itself
wraps the :py:class:`~parsl.data_provider.files.File` and :py:class:`~parsl.app.futures.DataFuture`
objects. However, this class is not intended to be used directly, and is only
mentioned here for completeness. This wrapping is done automatically when you
add an item to the list by direct assignment, ``append``, or ``extend``. Any
access to the members of the list is passed by the
:py:class:`~parsl.data_provider .dynamic_files.DynamicFileList.DynamicFile`
instance directly to the underlying object, thus making it an invisible layer to
the user.

The :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList.DynamicFile`
can also be instantiated with `None`,creating a placeholder for a
:py:class:`~parsl.data_provider.files.File` or :py:class:`~parsl.app.futures.DataFuture`.
This was done to resolve any indexing issues arrising from the dynamic nature
of the list. For example, without wrapping items in a
:py:class:`~parsl.data_provider.dynamic_files.DynamicFileList.DynamicFile` the
following code will not work:

.. code-block:: python

    from parsl.data_provider.dynamic_files import DynamicFileList
    from parsl.data_provider.files import File

    dfl = DynamicFileList()                    # dfl = []
                                               #          A       B       C
    dfl[2] = File('file://path/to/file')       # dfl = [File(), File(), File('file://path/to/file')]

    myfile = dfl[0]                            # myfile is a reference to the File() labeled A in dfl

    dfl[0] = File('file://path/to/file.2')     # dfl = [File('file://path/to/file.2'), File(),
                                               #        File('file://path/to/file')]
                                               # but myfile still points to the File() labeled A

    fh = open(myfile, 'r')                     # fails because myfile points to an empty File


But by utilizing the :py:class:`~parsl.data_provider.dynamic_files.DynamicFileList.DynamicFile` to wrap items,
the code will now work:

.. code-block:: python

    from parsl.data_provider.dynamic_files import DynamicFileList
    from parsl.data_provider.files import File

    dfl = DynamicFileList()                    # dfl = []
                                               #          A               B              C
    dfl[2] = File('file://path/to/file')       # dfl = [DynamicFile(), DynamicFile(), DynamicFile(File('file://path/to/file'))]

    myfile = dfl[0]                            # myfile is a reference to the DynamicFile() labeled A in dfl

    dfl[0] = File('file://path/to/file.2')     # dfl = [DynamicFile(File('file://path/to/file.2')),
                                               #        File(),
                                               #        File('file://path/to/file')]
                                               # myfile still points to the DynamicFile() labeled A, which now contains a file

    fh = open(myfile, 'r')                     # works


Staging providers
-----------------

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
