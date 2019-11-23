Apps
====

A Parsl **app** is a piece of code that can be asynchronously executed on an execution resource.
An **execution resource** in this context is any target system such as a laptop, cluster, cloud, or even supercomputer. Execution on these resources can be performed by a pool of `threads <https://en.wikipedia.org/wiki/Thread_(computing)>`_, `processes <https://en.wikipedia.org/wiki/Process_(computing)>`_, or remote workers.

Parsl apps are defined by annotating Python functions with an app decorator. Currently two types of apps can be defined: Python, with the ``@python_app`` decorator, and Bash, with the ``@bash_app`` decorator. Python apps encapsulate pure Python code, while Bash apps wrap calls to external applications and scripts.

Python Apps
-----------

The following code snippet shows a Python function ``double(x: int)``, used to double the input value. This function is defined as a Parsl Python app by using the ``@python_app`` decorator.

A Parsl Python app is a *pure* Python function. As it is executed asynchronously, and potentially remotely, it must explicitly import any required modules and act only on defined input arguments (i.e., it cannot include variables used outside the function).

.. code-block:: python

       @python_app
       def double(x):
             return x * 2

       double(x)

This Python app acts directly on the input argument `x`, which 
may be a Python object (hopefully, an integer) or a DataFuture (see :ref:`label-futures`) returned by another app. 
In the latter case, Parsl will wait until the future is resolved before executing the app.

A Python app may also act upon files. In order to make Parsl aware of these files, they must be specified by using the ``inputs`` and/or ``outputs`` keyword arguments, as in following code snippet, which copies the contents of one file (`in.txt`) to another (`out.txt`).

.. code-block:: python

       @python_app
       def echo(inputs=[], outputs=[]):
             with open(inputs[0], 'r') as in_file, open(outputs[0], 'w') as out_file:
                 out_file.write(in_file.readline())

       echo(inputs=[in.txt], outputs=[out.txt])

Limitations
^^^^^^^^^^^

There are limitations on what Python functions can be converted to apps:

1. Functions should act only on defined input arguments. That is, they should not use script-level or global variables.
2. Functions must explicitly import any required modules.
3. Parsl uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ and pickle to serialize Python objects to/from functions. Therefore, Python apps can only use input and output objects that can be serialized by cloudpickle or pickle.
4. STDOUT and STDERR produced by Python apps remotely are not captured.

Special Keyword Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^^

Any Parsl app (a Python function decorated with the ``@python_app`` or ``@bash_app`` decorator) can use the following special reserved keyword arguments.

1. inputs: (list) This keyword argument defines a list of input :ref:`label-futures` or files. 
   Parsl will wait for the results of any listed :ref:`label-futures` to be resolved before executing the app.
   The ``inputs`` argument is useful both for passing files as arguments
   and when one wishes to pass in an arbitrary number of futures at call time.
2. outputs: (list) This keyword argument defines a list of files that
   will be produced by the app. For each file thus listed, Parsl will create a future,
   track the file, and ensure that it is correctly created. The future 
   can then be passed to other apps as an input argument.
3. walltime: (int) If the app runs longer than ``walltime`` seconds, a `parsl.app.errors.AppTimeout` will be raised.

Returns
^^^^^^^

A Python app returns an AppFuture (see :ref:`label-futures`) as a proxy for the results that will be returned by the
app once it is executed. This future itself holds the Python object(s) returned by the app.
In case of an error or app failure, the future holds the exception raised by the app.

Bash Apps
---------

A Parsl Bash app is used to execute an external application or Bash script.
It is defined by a ``@bash_app`` decorator followed by a Python function that return a command-line string to be executed by Parsl.
For example, the following code snippet first defines and then calls a Bash app `echo_hello`,
which returns the string `'echo "Hello World!"'`. 
This string is a Bash command and will be executed as such.

.. code-block:: python

       @bash_app
       def echo_hello(stderr='std.err', stdout='std.out'):
           return 'echo "Hello World!"'

       # echo_hello() when called will execute the string it returns, creating an std.out file with
       # the contents "Hello World!"
       echo_hello()
       
Unlike a Python app, a Bash app cannot return Python objects.
Instead, it communicates with other functions by passing files.
The decorated ``@bash_app`` function provides the same ``inputs`` and ``outputs`` keyword arguments for managing input and output files.
It also includes, as described below, keyword arguments for capturing the STDOUT and STDERR streams and recording
them in files that are managed by Parsl.


Limitations
^^^^^^^^^^^

The following limitations apply to Bash apps:

1. Environment variables are not yet supported.

Special Keywords
^^^^^^^^^^^^^^^^

In addition to the ``inputs``, ``outputs``, and ``walltime`` argument keywords described above, a Bash app can take the following keywords:

4. stdout: (string or `parsl.AUTO_LOGNAME`) The path to a file to which standard output should be redirected. If set to `parsl.AUTO_LOGNAME`, the log will be automatically named according to task id and saved under `task_logs` in the run directory.
5. stderr: (string or `parsl.AUTO_LOGNAME`) The path to a file to which standard error should be redirected. If set to `parsl.AUTO_LOGNAME`, the log will be automatically named according to task id and saved under `task_logs` in the run directory.
6. label: (string) If the app is invoked with `stdout=parsl.AUTO_LOGNAME` or `stderr=parsl.AUTO_LOGNAME`, append `label` to the log name.

A Bash app allows for the composition of the string to execute on the command-line from the arguments passed
to the decorated function. The string that is returned is formatted by the Python string `format <https://docs.python.org/3.4/library/functions.html#format>`_  (`PEP 3101 <https://www.python.org/dev/peps/pep-3101/>`_).

.. code-block:: python

       @bash_app
       def echo(arg, inputs=[], stderr=parsl.AUTO_LOGNAME, stdout=parsl.AUTO_LOGNAME):
           return 'echo {} {} {}'.format(arg, inputs[0], inputs[1])

       future = echo('Hello', inputs=['World', '!'])
       future.result() # block until task has completed

       with open(future.stdout, 'r') as f:
           print(f.read()) # prints "Hello World !"


Returns
^^^^^^^

A Bash app, like a Python app, returns an AppFuture. 
However the value returned inside the AppFuture has no real meaning.

If a bash app exits with unix exit code 0, then the AppFuture will complete. If a bash app
exits with any other code, this will be treated as a failure, and the AppFuture will instead
contain an AppFailure exception. The Unix exit code can be accessed through the
`exitcode` attribute of that AppFailure.
