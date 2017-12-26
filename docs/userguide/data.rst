.. _label-data:

Data Management
===============

Parsl is designed to enable implementation of dataflow patterns. These patterns enable workflows to be defined in which the data passed between apps manages the flow of execution. Dataflow programming models are popular as they can cleanly express, via implicit parallelism, the concurrency needed by many applications in a simple and intuitive way. 

Parsl aims to abstract not only parallel execution but also execution location, which in turn requires abstractions for data location. This is crucial as it allows scripts to execute in different locations without regard for data location. Parsl implements a simple file abstraction that can be used to reference data. At present this model is limited to locally accessible files, but in the near future it will be extended to address remotely accessible files using HTTP and Globus protocols.

Files
-----

Parsl's file abstraction abstracts local access to a file. It therefore requires only the file path to be defined. Irrespective of where the script, or its apps are executed, Parsl uses this abstraction to access that file. When referencing a Parsl file in an app, Parsl maps the object to the appropriate access path.

The following example shows how Parsl files can be used in a simple ``cat`` app.

.. code-block:: python

       @App('bash', dfk)
       def cat(inputs=[], stdout='stdout.txt'):
            return 'cat %s' % (inputs[0])

       # create a test file
       open("test.txt", 'w').write('Hello\n')
       
       # create the Parsl file
       parsl_file = parsl.data_provider.files.File("test.txt")
      
       # call the cat app with the Parsl file
       cat(inputs=[parsl_file])
