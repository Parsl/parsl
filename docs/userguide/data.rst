.. _label-data:

Data management
===============

Parsl enables implementation of dataflow patterns in which data passed among apps manage the flow of execution. Dataflow programming models are popular as they can cleanly express, via implicit concurrency, opportunities for parallel execution.

Parsl abstracts not only parallel execution but also execution location. That is, it makes it possible for a Parsl app to execute anywhere, so that, for example, the following code will behave in the same way whether the `double` app is run locally or dispatched to a remote computer:

       @python_app
       def double(x):
             return x * 2

       double(x)

Achieving this location independence requires data location abstraction, so that a Parsl app receives the same input arguments, and can access files, in the same manner reqardless of its execution location.
To this end, Parsl:

* Orchestrates the movement of data passed as input arguments to a app, such as `x` in the above example, to whichever location is selected for that app's execution;

* Orchestrates the return value of any Python object returned by a Parsl Python object

* Implements a flexible file abstraction that can be used to reference data irrespective of its location. At present this model supports local files as well as files accessible via FTP, HTTP, HTTPS, and `Globus <https://globus.org>`_.

Files
-----

The :py:class:`~parsl.data_provider.files.File` class abstracts the file access layer. Irrespective of where a script or its apps are executed, Parsl uses this interface to access files. When referencing a Parsl file in an app, Parsl maps the object to the appropriate access path according to the selected URL *scheme*: Local, FTP, HTTP, HTTPS and Globus.


Local
^^^^^

The ``file`` scheme is used to reference files that are local to the computer on which the main program is executed.  A file using the local file scheme must specify the absolute file path, for example:

.. code-block:: python

        File('file://path/filename.txt')

The file may then be passed as input or output to an app. The following example executes the ``cat`` command on a local file:

.. code-block:: python

    @bash_app
    def cat(inputs=[], stdout='stdout.txt'):
         return 'cat %s' % (inputs[0])

    # create a test file
    open('/tmp/test.txt', 'w').write('Hello\n')

    # create the Parsl file
    parsl_file = File('file:///tmp/test.txt')

    # call the cat app, passing the Parsl file as an argument
    cat(inputs=[parsl_file])
    
The use of a Parsl file means that this program will behave in the same manner, regardless of where it is executed. Parsl handles the communications required for the `cat` app to access the contents of `/tmp/test.txt`, even if the app runs on a remote computer.

FTP, HTTP, HTTPS
^^^^^^^^^^^^^^^^

In other contexts, we may want a program to access data located on a remote server. File objects with FTP, HTTP, and HTTPS schemes represent remote files on FTP, HTTP and HTTPS servers, respectively. The following example defines a file accessible on a remote FTP server. 

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


Globus
^^^^^^

The ``Globus`` scheme is used to reference files that can be accessed using Globus (a guide to using Globus is available `here
<https://docs.globus.org/how-to/get-started/>`_). A file using the Globus scheme must specify the UUID of the Globus
endpoint and a path to the file on the endpoint, for example:

.. code-block:: python

        File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

Note: a Globus endpoint's UUID can be found in the Globus `Manage Endpoints <https://www.globus.org/app/endpoints>`_ page.

When Globus files are passed to/from an app as inputs or outputs, Parsl handles the mechanics of using Globus to stage the files to/from the remote location.
Thus, we can write programs such as the following that take a file on one Globus endpoint as input and produce a file on another Globus endpoint as output. This program sorts the strings in the first file, and writes the sorted output to the second file. Parsl handles the staging of the two files to and from the `sort_string` app's execution location.

.. code-block:: python

        @python_app
        def sort_strings(inputs=[], outputs=[]):
            with open(inputs[0].filepath, 'r') as u:
                strs = u.readlines()
                strs.sort()
                with open(outputs[0].filepath, 'w') as s:
                    for e in strs:
                        s.write(e)

        unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')
        sorted_file = File ('globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/sorted.txt')

        f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
        f.result()

Configuration
"""""""""""""

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

When using the Globus scheme, Parsl requires knowledge of the Globus endpoint that is associated with an executor. This is done by specifying the ``endpoint_uuid`` (the UUID of the Globus endpoint that is associated with the system) in the configuration.

In some cases, for example when using a Globus `shared endpoint <https://www.globus.org/data-sharing>`_ or when a Globus endpoint is mounted on a supercomputer, the path seen by Globus is not the same as the local path seen by Parsl. In this case the configuration may optionally specify a mapping between the ``endpoint_path`` (the common root path seen in Globus), and the ``local_path`` (the common root path on the local file system), as in the following.

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

Authorization
"""""""""""""

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
on an AWS EC2 instance.

Configuration
"""""""""""""

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

Authorization
"""""""""""""

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


