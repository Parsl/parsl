.. _label-data:

Data management
===============

Parsl is designed to enable implementation of dataflow patterns in which data passed between apps manages the flow of execution. Dataflow programming models are popular as they can cleanly express, via implicit parallelism,  the concurrency needed by many applications in a simple and intuitive way.

Parsl aims to abstract not only parallel execution but also execution location, which in turn requires data location abstraction. This is crucial as it allows scripts to execute in different locations without regard for data location. Parsl implements a simple file abstraction that can be used to reference data irrespective of its location. At present this model supports local files as well as files accessible via `Globus <https://globus.org>`_. In the near future it will be extended to address remotely accessible files using FTP and HTTP.

Files
-----

The :py:class:`~parsl.data_provider.files.File` class abstracts the file access layer. Irrespective of where the script or its apps are executed, Parsl uses this abstraction to access that file. When referencing a Parsl file in an app, Parsl maps the object to the appropriate access path according to the selected access *scheme*. Local and Globus schemes are supported, and are described in more detail below.


Local
^^^^^

The `file` scheme is used to reference local files.  A file using the local file scheme must specify the absolute file path, for example: 

.. code-block:: python

        File('file://path/filename.txt')

The file may then be passed as input or output to an app. Here is an example Parsl script which runs `cat` on a local file:

.. code-block:: python

       @App('bash', dfk)
       def cat(inputs=[], stdout='stdout.txt'):
            return 'cat %s' % (inputs[0])

       # create a test file
       open('test.txt', 'w').write('Hello\n')

       # create the Parsl file
       parsl_file = File('file://test.txt')

       # call the cat app with the Parsl file
       cat(inputs=[parsl_file])

Globus
^^^^^^

.. caution::
   This feature is available from Parsl ``v0.5.0`` in an ``experimental`` state.
   We request feedback and feature enhancement requests via `github <https://github.com/Parsl/parsl/issues>`_.


The `globus` scheme is used to reference files that can be accessed using Globus (a guide to using Globus is available `here
<https://docs.globus.org/how-to/get-started/>`_). A file using the Globus scheme must specify the UUID of the Globus
endpoint and a path to the file on the endpoint, for example:

.. code-block:: python

        File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

Note: the Globus endpoint UUID can be found in the Globus `Manage Endpoints <https://www.globus.org/app/endpoints>`_ page. 

Like the local file scheme, Globus files may be passed as input or output to a Parsl app. However, in the Globus case, the file object is only an abstract representation of the file on the remote side and thus the file must be staged to or from the remote executor.  For example, to stage in (transfer a file to the remote executor where the Parsl app will be executed), the :py:meth:`~parsl.data_provider.files.File.stage_in` and :py:meth:`~parsl.app.DataFuture.result` functions must be executed explicitly, for example:

.. code-block:: python

        unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

        dfu = unsorted_file.stage_in()
        dfu.result()

To stage a file out (transfer a file from the remote executor where the Parsl app is executed), the :py:meth:`~parsl.data_provider.files.File.stage_out` and :py:meth:`~parsl.app.DataFuture.result` functions must be executed explicitly, for example:

.. code-block:: python

        f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
        f.result()

        dfs = sorted_file.stage_out()
        dfs.result()
        
        
Parsl scripts may combine staging of files in and out of apps. For example, the following script stages a file from a remote Globus endpoint, it then sorts the strings in that file, and stages the sorted output file to another remote endpoint.  

.. code-block:: python

        @App('python', dfk)
        def sort_strings(inputs=[], outputs=[]):
            with open(inputs[0], 'r') as u:
                strs = u.readlines()
                strs.sort()
                with open(outputs[0].filepath, 'w') as s:
                    for e in strs:
                        s.write(e)


        unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')
        sorted_file = File ('globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/sorted.txt')

        dfu = unsorted_file.stage_in()
        dfu.result()

        f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
        f.result()

        dfs = sorted_file.stage_out()
        dfs.result()



Configuration
^^^^^^^^^^^^^

To inform Parsl where the file is to be transferred to or from (i.e., where the Parsl app is executed), the configuration must specify the `endpoint_name` (the UUID of the Globus endpoint that is associated with the system where the parsl app is executed). 

In order to manage where data is staged users may configure the default `working_dir` on a remote executor. This is passed to the :class:`~parsl.executors.ParslExecutor` via the `working_dir` parameter in the :class:`~parsl.config.Config` instance. For example:

.. code-block:: python

        from parsl.config import Config
        from parsl.executors.ipp import IPyParallelExecutor

        config = Config(
            executors=[
                IPyParallelExecutor(
                    working_dir="/home/user/parsl_script"
                )
            ]
        )

In some cases, for example when using a Globus `shared endpoint <https://www.globus.org/data-sharing>`_ or when a Globus DTN is mounted on a supercomputer, the path seen by Globus is not the same as the local path seen by Parsl. In this case the configuration may optionally specify a mapping between the `endpoint_path` (the common root path seen in Globus), and the `local_path` (the common root path on the local file system). In most cases `endpoint_path` and `local_path` are the same. 

.. code-block:: python

        from parsl.config import Config
        from parsl.executors.ipp import IPyParallelExecutor
        from parsl.data_manager.scheme import GlobusScheme

        config = Config(
            executors=[
                IPyParallelExecutor(
                    working_dir="/home/user/parsl_script",
                    storage_access=GlobusScheme(
                        endpoint_uuid="7d2dc622-2edb-11e8-b8be-0ac6873fc732",
                        endpoint_path="/",
                        local_path="/home/user"
                    )
                )
            ]
        )
