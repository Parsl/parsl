Apps
====

In Parsl an "app" is a piece of code that can be asynchronously executed on an execution resource.
An execution resource in this context is any target system such as a laptop, cluster, cloud, or even supercomputer. Execution on these resources can be performed by a pool of `threads <https://en.wikipedia.org/wiki/Thread_(computing)>`_, `processes <https://en.wikipedia.org/wiki/Process_(computing)>`_, or remote workers.

Parsl apps are defined by annotating Python functions with the ``@App`` decorator. Currently two different types of apps can be defined: Python and Bash. Python apps encapsulate pure Python code, while Bash apps wrap calls to external applications and scripts.

Python Apps
-----------

The following code snippet shows a simple Python function used to double the input value (``double(Int)``). This function is defined as a Parsl app using the ``@App`` decorator.
The first argument to ``@App`` specifies the App type as "python". The second argument ``dfk`` is the Dataflow Kernel which must be configured with appropriate execution resources (e.g., local thread execution).

Python apps are *pure* Python functions. As these functions are executed asynchronously, and potentially remotely, it is important to note that they must explicitly import any required modules and act only on defined input arguments (i.e., it cannot include variables used elsewhere in the script).

.. code-block:: python

       @App('python', dfk)
       def double(x):
             return x*2

       double(x)

Python apps may also act upon files. In order to make these files known to Parsl's Dataflow Kernel you must define the input and output files to be managed. The following code illustrates a simple example that will copy the contents of one file to another.

.. code-block:: python

       @App('python', dfk)
       def echo(inputs=[], outputs=[]):
             with open(inputs[0], 'r') as in_file, open(outputs[0], 'w') as out_file:
                 out_file.write(in_file.readline())

       echo(inputs=[in.txt], outputs=[out.txt])

Limitations
^^^^^^^^^^^

There are limitations on what Python functions can be converted to apps:

1. Functions should act only on defined input arguments.
2. Functions must explicitly import any required modules.
3. Functions should not use script-level or global variables.
4. Parsl uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ and pickle to serialize Python constructs, such as inputs and outputs to functions. Therefore, Python apps can only use inputs and outputs that can be serialized by cloudpickle or pickle.

Special Keywords Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^^

Any Parsl app (decorated with the ``@App`` decorator) can use the following special reserved keyword arguments.

1. inputs : (list) This keyword argument allows you to pass a list of input :ref:`label-futures`, and thus wait on
   the results of these futures to be resolved before execution.
2. outputs : (list) This keyword argument allows you to explicitly list the output :ref:`label-futures` that
will be produced by this app. Parsl will track these files and ensure they are correctly created. They can then be passed to other apps as input arguments.

Returns
^^^^^^^

A python app returns an AppFuture that is a proxy for the results that will be returned by the
app once it is executed. This futures itself holds the python object(s) returned by the app.
In case of a failure in the app, the future holds the exception raised by the app.

Bash Apps
---------

Parsl's Bash app allows you to wrap execution of external applications from the command-line as you would in a Bash shell. It can also be used to execute Bash scripts directly. To define a Bash app the wrapped Python function must return the command-line string to be executed.

The following code snippet shows a simple Bash script written as a string in Python and wrapped as an app.
Any command-line invocation represented by an arbitrarily long string, can be returned by a function decorated
within an ``@App`` of type `bash` to be executed. Unlike the Python app, Bash apps communicate by passing files.
The decorated `bash` function provides the same special keyword arguments to manage input and output files.
In addition, it also includes keyword arguments for capturing the STDOUT and STDERR streams and recording
them in files that are managed by Parsl.


.. code-block:: python

       @App('bash', dfk)
       def echo_hello(stderr='std.err', stdout='std.out'):
           return 'echo "Hello World!"'

       # echo_hello() when called will execute the string it returns, creating an std.out file with
       # the contents "Hello World!"
       echo_hello()


Limitations
^^^^^^^^^^^

The following limitations apply to Bash apps:

1. Environment variables are not yet supported.

Special Keywords
^^^^^^^^^^^^^^^^

1. inputs: (list) A list of input :ref:`label-futures` on which to wait before execution.
2. outputs: (list) A list of output :ref:`label-futures` that will be created by the app.
3. stdout: (string) The path to a file to which STDOUT should be redirected.
4. stderr: (string) The path to a file to which STDERR should be redirected.

The Bash app allows a user to compose the string to execute on the command-line from the various arguments passed
to the decorated function. The string that is returned is formatted by the Python string `format <https://docs.python.org/3.4/library/functions.html#format>`_  (`PEP 3101 <https://www.python.org/dev/peps/pep-3101/>`_).

.. code-block:: python

       @App('bash', dfk)
       def echo(arg1, inputs=[], stderr='std.err', stdout='std.out'):
           return 'echo %s %s %s' % (arg1, inputs[0], inputs[1])

       # This call echoes "Hello World !" to the file *std.out*
       echo('Hello', inputs=['World', '!'])

Returns
^^^^^^^

A bash app returns an AppFuture just like a python app however the values returned by the
future are quite different. In Unix fashion, the result made available upon
completion is the **return/exit code** of the bash script. This future may also hold various
exceptions that capture errors during execution such as incorrect privileges, missing output
files etc.
