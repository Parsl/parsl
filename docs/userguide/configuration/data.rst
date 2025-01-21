.. _label-data:

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


.. _label-dynamic-file-list:

Dynamic File List
^^^^^^^^^^^^^^^^^

In certain cases, you do not know the number of files that will be produced by an app. When this happens, your script
will likely fail with an error ro do something unexpected, as Parsl needs to know about all files (input and output)
before an app runs. For the circumstances the :py:class:`parsl.data_provider.dynamic_files.DynamicFileList` was
developed. The purpose of the :py:class:`parsl.data_provider.dynamic_files.DynamicFileList` is to allow an app to add
files to the outputs, without the outputs being pre-specified. The
:py:class:`parsl.data_provider.dynamic_files.DynamicFileList` behaves like a list, but is also a Future. It can be used
anywhere a list of :py:class:`~parsl.data_provider.files.File` objects is expected.

Take this example:

.. code-block:: python

    import parsl
    from parsl.config import Config
    from parsl.executors import ThreadPoolExecutor
    from parsl.data_provider.dynamic_files import DynamicFileList

    config = Config(executors=[ThreadPoolExecutor(label="local_htex")])
    parsl.load(config)

    @parsl.python_app
    def produce(outputs=[]):
        import random
        import string
        from parsl.data_provider.files import File
        count = random.randint(3, 9)
        for i in range(count):
            fl = File(f'data_{i}.log')
            with open(fl, 'w') as fh:
                fh.write(''.join(random.choices(string.ascii_letters, k=50)))
            outputs.append(fl)
        print(f"\n\nProduced {len(outputs)} files")

    @parsl.python_app
    def consume(inputs=[]):
        from parsl.data_provider.files import File
        print(f"Consuming {len(inputs)} files")
        for inp in inputs:
            with open(inp.filepath, 'r') as inp:
                print(f"  Reading {inp}")
                content = inp.read()

The app ``produce`` produces a random number of output files, these could be log files, data files, etc.). The
``consume`` function takes those files and reads them (it could really do anything with them). If we use the following
code, we wil not get the expected result:

.. code-block:: python

    outp = []
    prc = produce(outputs=outp)
    cons = consume(inputs=outp)
    cons.result()

The code will output something like

.. code-block:: bash

    Consuming 0 files
    Produced 3 files

which is both out of order (the ``Produced`` line should be first) and incorrect (the ``Consuming`` line should have the
same number as the ``Produced`` line). This is because when Parsl generates the DAG, it sees an empty list for the
``inputs`` to consume and believes that there is no connection between the ``outputs`` of ``produce`` and these
``inputs``. Thus generating a DAG that allows ``consume`` to run whenever there are processing resources available, even
parallel with ``produce``. If we make a single line change (changing ``outp`` to be a
:py:class:`parsl.data_provider.dynamic_files.DynamicFileList`), this can be fixed:

.. code-block:: python

    outp = DynamicFileList()
    prc = produce(outputs=outp)
    cons = consume(inputs=outp)
    cons.result()

The code will now work properly, reporting the correct number of files produced and consumed:

.. code-block:: bash

    Produced 7 files
    Consuming 7 files
      Reading <_io.TextIOWrapper name='data_0.log' mode='r' encoding='UTF-8'>
      Reading <_io.TextIOWrapper name='data_1.log' mode='r' encoding='UTF-8'>
      Reading <_io.TextIOWrapper name='data_2.log' mode='r' encoding='UTF-8'>
      Reading <_io.TextIOWrapper name='data_3.log' mode='r' encoding='UTF-8'>
      Reading <_io.TextIOWrapper name='data_4.log' mode='r' encoding='UTF-8'>
      Reading <_io.TextIOWrapper name='data_5.log' mode='r' encoding='UTF-8'>
      Reading <_io.TextIOWrapper name='data_6.log' mode='r' encoding='UTF-8'>

This works because, as a Future, the :py:class:`parsl.data_provider.dynamic_files.DynamicFileList` causes Parsl to make
a connection between the ``outputs`` of ``produce`` and the ``inputs`` of ``consume``. This causes the DAG to wait until
``produce`` has completed before running ``consume``.

The :py:class:`parsl.data_provider.dynamic_files.DynamicFileList` can also be used in more complex ways, such as slicing
and will behave as expected. Lets take the previous example where ``produce`` generates an unknown number of files. You
know that the first one produced is always a log file, which you don't really care about, but the remaining files are
data that you are interested in. Traditionally you would do something like

.. code-block:: python

    outp = []
    prc = produce(outputs=outp)
    cons = consume(inputs=outp[1:])
    cons.result()

but this will either throw an exception or fail (depending on your Python version) as the first example above did with 0
consumed files. But using a :py:class:`parsl.data_provider.dynamic_files.DynamicFileList` will work as expected:

.. code-block:: python

    outp = DynamicFileList()
    f1 = process(outputs=outp)
    r1 = consume(inputs=outp[1:])
    r1.result()

.. _label-bash-watcher:

None of the above examples will necessarily work as expected if ``produce`` was a ``bash_app``. This is because the
command line call returned by the ``bash_app`` may produce files that neither Python nor Parsl are aware of, there is no
direct way to to know what files to track, without additional work. Parsl provides a function that can help with this.
The :py:func:`parsl.app.watcher.bash_watcher` can be used to wrap and watch for files produced by a ``bash_app``. The
``bash_app`` being wrapped does not need to change, just the way it is called. In the following example assume that
there is a ``bash_app`` named my_func that takes two arguments and produces some files. Normally you would call it like
this:

.. code-block:: python

    outp = [File('file1.txt'), File('file2.txt')]
    a = my_func(my_arg1, my_arg2, outputs=outp)

But if you don't know what files may be produced, you can use the :py:func:`parsl.app.watcher.bash_watch` like this:

.. code-block:: python

    from parsl.app.watcher import bash_watcher
    from parsl.data_provider.dynamic_files import DynamicFileList

    outp = DynamicFileList()
    a = bash_watcher(my_func, outp, os.getcwd(), my_arg1, my_arg2)
    res = a.result()

The first argument is the ``bash_app`` to be watched. The second is a :py:class:`parsl.data_provider.dynamic_files.DynamicFileList`
instance which will contain the files produced by the ``bash_app``. The third argument is the path or paths (as a list)
of directories to watch, recursively, for new files in, the default is ".". It is recommended that the directory
structure being watch is not overly complex as this can slow down the watcher. The remaining arguments are the same as
those that would be passed to the ``bash_app``, both positional and keyword. The ``result()`` function must be called on
the ``bash_watcher`` or Parsl may get into a deadlock situation.

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
