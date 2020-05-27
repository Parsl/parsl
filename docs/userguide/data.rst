.. _label-data:

Data management
===============

Parsl supports staging of file data between execution locations and at-rest
locations, specifically staging inputs from at-rest locations to execution locations,
and staging outputs from execution locations to at-rest locations. 
This supports two pieces of functionality: execution location
abstraction, and ordering of execution by data flow.

Parsl abstracts not only parallel execution but also execution location. That is, it makes it possible for a Parsl app to execute anywhere, so that, for example, the following code will behave in the same way whether the `double` app is run locally or dispatched to a remote computer:

       @python_app
       def double(x):
             return x * 2

       double(x)

Achieving this location independence requires data location abstraction, so that a Parsl app receives the same input arguments, and can access files, in the same manner reqardless of its execution location.
To this end, Parsl:

* Orchestrates the movement of data passed as input arguments to a app, such as `x` in the above example, to whichever location is selected for that app's execution;

* Orchestrates the return value of any Python object returned by a Parsl Python object

* Implements a flexible file abstraction that can be used to reference data irrespective of its location. At present this model supports local files as well as files accessible on the submit-side filesystem
or via FTP, HTTP, HTTPS, and `Globus <https://globus.org>`_.

In a workflow, file handling is split into two pieces: files are named in an
execution-location independent manner using :py:class:`~parsl.data_provider.files.File`
objects, and executors are configured to stage those files in to and out of
execution locations using instances of the :py:class:`~parsl.data_provider.staging.Staging`
interface.

Using Files in a workflow
-------------------------

Parsl can stage files in for Apps to use as inputs, and stage files out that
Apps have produced. These files must be identified before invoking the App
by creating :py:class:`~parsl.data_provider.files.File` instances naming the
at-rest location of the file. This can be a path referring to the submit-side
file system, or more generally a URL.

Input files can be passed as regular arguments, or a list of them may be
specified in the special `inputs` keyword argument to an app invocation.

Inside an app, the `filepath` attribute of a `File` can be read to determine
where on the execution-side filesystem the input file has been placed.

Output file objects must also be passed in at app invocation, through the
outputs parameter. Inside an app, the `filepath` attribute of an output
`File` provides the path at which the corresponding output file should be
placed so that the stage-out code can find it after execution.

If the output from an app is to be used as the input to a subsequent app,
then a DataFuture that represents whether the output file has been created
must be extracted from the first app's AppFuture, and that must be passed
instead of the original File object to the second app. This causes app
executions to be properly ordered, in the same way that passing AppFutures
to subsequent apps causes execution ordering based on an app returning.


Staging providers
-----------------

Each executor can be configured with a list of
:py:class:`~parsl.data_provider.staging.Staging` instances
that will be used to stage files in and out of execution
locations. This list should be supplied as the `storage_access`
parameter to an executor when it is constructed as part of a
Parsl configuration.

Parsl comes with several staging providers, some of which are
enabled by default: the shared file system provider and the HTTP(S)
and FTP separate task staging providers.

NoOpFileStaging for Local/Shared File Systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The NoOpFileStaging provider assumes that files specified either
with a path or with the ``file`` URL scheme are available both
on the submit and execution side - this occurs, for example, when there is a
shared file system. 

FTP, HTTP, HTTPS: separate task staging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Files named with an ``ftp``, ``http`` or ``https`` URL will be
staged in using HTTP GET or anonymous FTP executed as a separate
Parsl task that will complete before the corresponding App
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

FTP, HTTP, HTTPS: in-task staging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These staging providers are intended for use on executors that do not have
a file system shared between each executor node.

These providers will use the same HTTP GET/anonymous FTP as the separate
task staging providers described above, but will do so in a wrapper around
individual app invocations, which guarantees that they will stage files to
a filesystem visible to the app.

A downside of this is that the staging tasks are less visible to parsl, as
they are not performed as separate Parsl tasks.


Globus
^^^^^^

The ``Globus`` staging provider is used to transfer files that can be accessed
using Globus. A guide to using Globus is available `here
<https://docs.globus.org/how-to/get-started/>`_).

A file using the Globus scheme must specify the UUID of the Globus
endpoint and a path to the file on the endpoint, for example:

.. code-block:: python

        File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

Note: a Globus endpoint's UUID can be found in the Globus `Manage Endpoints <https://www.globus.org/app/endpoints>`_ page.

There must also be a Globus endpoint available with access to a
execute-side shared file system, because Globus file transfers happen
between two Globus endpoints.

Globus Configuration
^^^^^^^^^^^^^^^^^^^^

In order to manage where data are staged, users may configure the default ``working_dir`` on a remote location. This information is passed to the :class:`~parsl.executors.ParslExecutor` via the `working_dir` parameter in the :class:`~parsl.config.Config` instance. For example:

.. code-block:: python

        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor

        config = Config(
            executors=[
                HighThroughputExecutor(
                    working_dir="/home/user/parsl_script"
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
        
However, in most cases, ``endpoint_path`` and ``local_path`` are the same.

Globus Authorization
""""""""""""""""""""

In order to interact with Globus, you must be authorised. The first time that
you use Globus with Parsl, prompts will take you through an authorization
procedure involving your web browser. You can authorize without having to
run a script (for example, if you are running your script in a batch system
where it will be unattended) by running this command line:

.. code-block:: bash

        $ parsl-globus-auth
        Parsl Globus command-line authoriser
        If authorisation to Globus is necessary, the library will prompt you now.
        Otherwise it will do nothing
        Authorization complete

rsync
^^^^^

The `rsync` utility can be used to transfer files in the `file:` scheme in configurations where
workers cannot access the submit side filesystem directly, such as when executing
on an AWS EC2 instance. Instead, the submit side filesystem must be exposed using
rsync.

rsync Configuration
"""""""""""""""""""

`rsync` must be installed on both the submit and worker side. It can usually be installed
by using the operating system package manager: for example, by `apt-get install rsync`.

An `RSyncStaging` option must then be added to the Parsl configuration file, as in the following.
The parameter to RSyncStaging should describe the prefix to be passed to each rsync
command to connect from workers to the submit side host. This will often be the username
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
underlying `rsync` command. This command must be correctly authorized to connect back to 
the submitting system. The form of this authorization will depend on the systems in 
question.

The following example installs an ssh key from the submit side filesystem and turns off host key 
checking, in the `worker_init` initialization of an EC2 instance. The ssh key must have 
sufficient privileges to run `rsync` over ssh on the submitting system.

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


