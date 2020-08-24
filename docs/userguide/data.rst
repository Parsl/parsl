.. _label-data:

Communication and data
======================

Communication between the Parsl program and Parsl tasks is crucial
for accomplishing necessary work. Parsl supports two modes of communication: 
by standard Python parameters and by files.

Parsl abstracts not only concurrent execution but also the location in which
an app executes. That is, it makes it possible for a Parsl app to execute anywhere, that is 
a Parsl app will behave in the same way whether it is run locally or dispatched to a remote 
computer. 
Achieving location independence requires data location abstraction, so that a Parsl app receives the same input arguments, and can access files, in the same manner regardless of its execution location.
To this end, Parsl:

* Orchestrates the movement of Python parameters or files passed as input arguments to an app, to whichever location is selected for that app's execution;

* Orchestrates the return value of any Python object or exception returned by a Parsl app; 

* Serializes and deserializes Python data types, objects, and exceptions;

* Implements a flexible file abstraction that can be used to reference files irrespective of their locations. At present this model supports local files as well as files accessible on the submit-side file system or via FTP, HTTP, HTTPS, and `Globus <https://globus.org>`_;

* Translates file paths to location-specific paths relative to the location in which the app executes.


Communicating via Python
------------------------

Parsl apps can communicate via standard Python function parameter passing 
and return statements. The following example shows how a Python string
can be passed to, and returned from, a Parsl app.

.. code-block:: python

    @python_app
    def communicate(name):
        return 'hello {0}'.format(name)
				
    r = communicate('bob')
    print(r.result())

Parsl uses the cloudpickle and pickle libraries to serialize Python objects 
into a sequence of bytes that can be passed over a network to/from apps. 
Thus, Parsl is able to support communication via standard Python data types 
(e.g., booleans, integers, tuples, lists, and dictionaries). However, not
all objects can be serialized with these methods (e.g., closures, generators, 
and system objects). 

Parsl will raise a `SerializationError` if it encounters an object that it cannot 
serialize. This applies to objects passed as arguments to an app, as well as objects 
returned from an app. See :ref:`label_serialization_error`.


Communicating via Files
-----------------------

Parsl apps can communicate via files. A file may be passed as an input
argument to an app, or returned from an app after execution. Parsl 
provides support to automatically transfer (stage) files between 
the main Parsl program, worker nodes, or external data storage systems. 

Input files can be passed as regular arguments, or a list of them may be
specified in the special ``inputs`` keyword argument to an app invocation.

Inside an app, the ``filepath`` attribute of a `File` can be read to determine
where on the execution-side file system the input file has been placed.

Output file objects must also be passed in at app invocation, through the
outputs parameter. Inside an app, the ``filepath`` attribute of an output
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
    def print_file(inputs=[]):
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
    def convert(inputs=[], outputs=[]):
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
