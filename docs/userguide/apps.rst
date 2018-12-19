Apps
====

In Parsl an "app" is a piece of code that can be asynchronously executed on an execution resource.
An execution resource in this context is any target system such as a laptop, cluster, cloud, or even supercomputer. Execution on these resources can be performed by a pool of `threads <https://en.wikipedia.org/wiki/Thread_(computing)>`_, `processes <https://en.wikipedia.org/wiki/Process_(computing)>`_, or remote workers.

Parsl apps are defined by annotating Python functions with an app decorator. Currently two types of apps can be defined: Python, with the corresponding ``@python_app`` decorator, and Bash, with the corresponding ``@bash_app`` decorator. Python apps encapsulate pure Python code, while Bash apps wrap calls to external applications and scripts.

Python Apps
-----------

The following code snippet shows a Python function ``double(int)``, used to double the input value. This function is defined as a Parsl app using the ``@python_app`` decorator.

Python apps are *pure* Python functions. As these functions are executed asynchronously, and potentially remotely, it is important to note that they must explicitly import any required modules and act only on defined input arguments (i.e., they cannot include variables used elsewhere in the script).

.. code-block:: python

       @python_app
       def double(x):
             return x * 2

       double(x)

Python apps may also act upon files. In order to make Parsl aware of these files they must be defined using the inputs or outputs keyword arguments. The following code snippet illustrates how the contents of one file can be copied to another.

.. code-block:: python

       @python_app
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
4. Parsl uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ and pickle to serialize Python objects to/from functions. Therefore, Python apps can only use input and output objects that can be serialized by cloudpickle or pickle.
5. STDOUT and STDERR produced by Python apps remotely are not captured.

Special Keyword Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^^

Any Parsl app (a Python function decorated with the ``@python_app`` or ``@bash_app`` decorator) can use the following special reserved keyword arguments.

1. inputs: (list) This keyword argument defines a list of input :ref:`label-futures`. Parsl will establish a dependency on these inputs and wait for the results of these futures to be resolved before execution.    This is useful if one wishes to pass in an arbitrary number of futures at call
   time; note that if :ref:`label-futures` are passed as positional arguments, they will also be resolved before execution.
2. outputs: (list) This keyword argument defines a list of output :ref:`label-futures` that
   will be produced by this app. Parsl will track these files and ensure they are correctly created.
   They can then be passed to other apps as input arguments.

Returns
^^^^^^^

A Python app returns an AppFuture that is a proxy for the results that will be returned by the
app once it is executed. This future itself holds the python object(s) returned by the app.
In case of an error or app failure, the future holds the exception raised by the app.

Bash Apps
---------

Parsl's Bash app is used to wrap the execution of external applications from the command-line. It can also be used to execute Bash scripts directly. To define a Bash app the wrapped Python function must return the command-line string to be executed.

The following code snippet shows a Bash app that will print a message to stdout.
Any command-line invocation represented by an arbitrarily long string can be returned by a function decorated
within a ``@bash_app`` to be executed. Unlike Python apps, Bash apps cannot return Python objects, instead
they communicate by passing files.
The decorated ``@bash_app`` function provides the same inputs and outputs keyword arguments to manage input and output files.
It also includes keyword arguments for capturing the STDOUT and STDERR streams and recording
them in files that are managed by Parsl.


.. code-block:: python

       @bash_app
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

A Bash app allows for the composition of the string to execute on the command-line from the arguments passed
to the decorated function. The string that is returned is formatted by the Python string `format <https://docs.python.org/3.4/library/functions.html#format>`_  (`PEP 3101 <https://www.python.org/dev/peps/pep-3101/>`_).

.. code-block:: python

       @bash_app
       def echo(arg1, inputs=[], stderr='std.err', stdout='std.out'):
           return 'echo %s %s %s' % (arg1, inputs[0], inputs[1])

       # This call echoes "Hello World !" to the file *std.out*
       echo('Hello', inputs=['World', '!'])

Returns
^^^^^^^

A Bash app returns an AppFuture just like a Python app, however the values returned by the
future are different. The result made available upon
completion is the **return/exit code** of the Bash script. This future may also hold various
exceptions that capture errors during execution such as incorrect privileges, missing output
files, etc.
