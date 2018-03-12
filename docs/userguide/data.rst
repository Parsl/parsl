.. _label-data:

Data Management
===============

Parsl is designed to enable implementation of dataflow patterns. These patterns enable workflows to be defined in which the data passed between apps manages the flow of execution. Dataflow programming models are popular as they can cleanly express, via implicit parallelism, the concurrency needed by many applications in a simple and intuitive way.

Parsl aims to abstract not only parallel execution but also execution location, which in turn requires abstractions for data location. This is crucial as it allows scripts to execute in different locations without regard for data location. Parsl implements a simple file abstraction that can be used to reference data. At present this model is limited to locally accessible files, but in the near future it will be extended to address remotely accessible files using FTP, HTTP, and Globus protocols.

Files
-----

Parsl's file abstraction abstracts local access to a file. It therefore requires only the file path to be defined. Irrespective of where the script, or its apps are executed, Parsl uses this abstraction to access that file. When referencing a Parsl file in an app, Parsl maps the object to the appropriate access path.

The following example shows how Parsl files can be used in a simple ``cat`` app.

.. code-block:: python

       @App('bash', dfk)
       def cat(inputs=[], stdout='stdout.txt'):
            return 'cat %s' % (inputs[0])

       # create a test file
       open('test.txt', 'w').write('Hello\n')

       # create the Parsl file
       parsl_file = parsl.data_provider.files.File('test.txt')

       # call the cat app with the Parsl file
       cat(inputs=[parsl_file])

Explicit Staging
----------------

When an app needs to access a remote file that is specified as an input or output of the app, Parsl makes sure that the file is stage to a site where the app is executed. In many cases, a user may want to stage all files before any app is executed to avoid apps in the middle of the workflow will wait their input files to be staged.
The following example shows how files on remote Globus endpoints can be specified as an app input and output and how to stage them in and out explicitely.

.. code-block:: python

        config = {
            "sites": [
                {
                    "data": {
                        "globus": {
                            "endpoint_name": "1afdfb30-1102-11e8-a7ed-0a448319c2f8",
                            "endpoint_path": "/parsl",
                            "local_directory": "/home/$USER/projects/share/parsl"
                        }
                    }
                }
            ]
        }

        @App('python', dfk)
        def sort_strings(inputs=[], outputs=[]):
            with open(inputs[0].filepath, 'r') as u:
                strs = u.readlines()
                strs.sort()
                with open(outputs[0].filepath, 'w') as s:
                    for e in strs:
                        s.write(e)

        
        unsorted_file = File('globus://037f054a-15cf-11e8-b611-0ac6873fc732/unsorted.txt')
        sorted_file = File ('globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/~/sorted.txt')

        unsorted_file.stage_in()

        f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
        f.result()

        sorted_file.stage_out()

If an app wants to read or write remote files on a globus endpoint, Parsl must know a UUID or name of an endpoint associated with a site the app is executed at to be able to stage the files to the site before the files can be open or staged them out after At least one of the sites specified in the config must include the "globus" attribute with "globus_endpoint" and a path that points to a directory at the site where remote files are to be staged. A root of a Globus endpoint may not match a root of the filesystem at the site. In this case, Parsl needs to know a directory on the endpoint that corresponds to "local_directory".
