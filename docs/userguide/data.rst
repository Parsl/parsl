.. _label-data:

Data Management
===============

Parsl is designed to enable implementation of dataflow patterns. These patterns enable workflows to be defined in which the data passed between apps manages the flow of execution. Dataflow programming models are popular as they can cleanly express, via implicit parallelism, the concurrency needed by many applications in a simple and intuitive way.

Parsl aims to abstract not only parallel execution but also execution location, which in turn requires abstractions for data location. This is crucial as it allows scripts to execute in different locations without regard for data location. Parsl implements a simple file abstraction that can be used to reference data. At present this model is limited to locally accessible files and Globus protocols, but in the near future it will be extended to address remotely accessible files using FTP, HTTP.

Files
-----

The :py:class:`~parsl.data_provider.files.File` class abstracts the file access layer. Irrespective of where the script or its apps are executed, Parsl uses this abstraction to access that file. When referencing a Parsl file in an app, Parsl maps the object to the appropriate access path according to the selected scheme. Local and Globus schemes are supported, and are described in more detail below.


Local
^^^^^

The `file` scheme refers to local files. Here is an example which runs `cat` on a local file:

.. code-block:: python

       @App('bash', dfk)
       def cat(inputs=[], stdout='stdout.txt'):
            return 'cat %s' % (inputs[0])

       # create a test file
       open('test.txt', 'w').write('Hello\n')

       # create the Parsl file
       parsl_file = parsl.data_provider.files.File('file://test.txt')

       # call the cat app with the Parsl file
       cat(inputs=[parsl_file])

Globus
^^^^^^

The `globus` scheme represents files that can be accessed using the `Globus Transfer Service
<https://docs.globus.org/how-to/get-started/>`_. A file using the Globus scheme must specify the UUID of the Globus
endpoint and a path to the file on the endpoint, for example:

.. code-block:: python

        File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')

The file object is only an abstract representation of the file on the remote side. To inform Parsl where the file is to be transferred (where the Parsl app will be executed), the user must provide a configuration (passed to the DataFlowKernel) which specifies the `endpoint_name` (the UUID of the Globus endpoint that is associated with the system where the parsl App will run), the `endpoint_path` (the path to the directory where the file will be staged in for the Parsl App), and the `local_directory` (the path on the local filesystem that corresponds with the local filesystem). In most cases `endpoint_path` and `local_directory` are the same. They are different only if the root directory on the endpoint is not the root directory of the filesystem. For example:

.. code-block:: python

        config = {
            "sites": [
                {
                    "data": {
                        "globus": {
                            "endpoint_name": "af7bda53-6d04-11e5-ba46-22000b92c6ec",
                            "endpoint_path": "/home/$USER/",
                            "local_directory": "/home/$USER/",
                        }
                    }
                }
            ]
        }


To stage in (transfer a file to the location where the Parsl app will be executed), the :py:meth:`~parsl.data_provider.files.File.stage_in` and :py:meth:`~parsl.app.DataFuture.result` method must be executed explicitly, for example:

.. code-block:: python

        unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/foo/bar/unsorted.txt')

        dfu = unsorted_file.stage_in()
        dfu.result()


Here is a full example:

.. code-block:: python

        @App('python', dfk)
        def sort_strings(inputs=[], outputs=[]):
            with open(inputs[0], 'r') as u:
                strs = u.readlines()
                strs.sort()
                with open(outputs[0].filepath, 'w') as s:
                    for e in strs:
                        s.write(e)


        unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/foo/bar/unsorted.txt')
        sorted_file = File ('globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/baz/sorted.txt')

        dfu = unsorted_file.stage_in()
        dfu.result()

        f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
        f.result()

        dfs = sorted_file.stage_out()
        dfs.result()

