Apps
====

A Parsl **app** is a piece of code that can be asynchronously executed on an execution resource.
(An **execution resource** in this context is any target system such as a laptop, cluster, cloud, or even supercomputer. Execution on these resources can be performed by a pool of `threads <https://en.wikipedia.org/wiki/Thread_(computing)>`_, `processes <https://en.wikipedia.org/wiki/Process_(computing)>`_, or remote workers.)

A Parsl app is defined by annotating a Python function with an app decorator. 
Currently two types of apps can be defined: a **Python app**, with the ``@python_app`` decorator, and a **Bash app**, with the ``@bash_app`` decorator. 
Python apps encapsulate pure Python code, while Bash apps wrap calls to external applications and scripts.

Python Apps
-----------

The following code snippet shows a Python function ``double(x: int)``, used to double the value provided as input. 
The ``@python_app`` decorator defines the function as a Parsl Python app.  

.. code-block:: python

       @python_app
       def double(x):
             return x * 2

       double(42)

As a Parsl Python app is executed asynchronously, and potentially remotely, it must explicitly import any required modules and cannot refer to variables used outside the function. 
Thus while the following code fragment is valid Python, it is not valid Parsl, as the `bad_double()` function requires the model `random` and refers to the external variable `factor`.

.. code-block:: python

       import random
       factor = 5

       @python_app
       def bad_double(x):
             return x * random.random() * factor

       print(bad_double(42))
       
The following alternative formulation is valid Parsl.

.. code-block:: python

       import random
       factor = 5

       @python_app
       def good_double(x, f):
             import random
             return x * random.random() * f

       print(good_double(42, factor))

An input argument 
may be a Python object or any `Future` (see :ref:`label-futures`) returned by another app.
In the latter case, Parsl will wait until the future is resolved before executing the app,
as we discuss in more detail in :ref:`label-futures`.

A Python app may also act upon files. In order to make Parsl aware of these files, they must be specified by using the ``inputs`` and/or ``outputs`` keyword arguments, as in following code snippet, which copies the contents of one file (`in.txt`) to another (`out.txt`).

.. code-block:: python

       @python_app
       def echo(inputs=[], outputs=[]):
             with open(inputs[0], 'r') as in_file, open(outputs[0], 'w') as out_file:
                 out_file.write(in_file.readline())

       echo(inputs=[in.txt], outputs=[out.txt])

Limitations
^^^^^^^^^^^

There are limitations on the Python functions that can be converted to apps:

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

The string thus executed by a Bash app can be arbitrarily long. 

Unlike a Python app, a Bash app cannot return Python objects.
Instead, it communicates with other functions by creating files.
A decorated ``@bash_app`` function provides the ``inputs`` and ``outputs`` keyword arguments for managing input and output files.
It also includes, as described below, keyword arguments for capturing the STDOUT and STDERR streams and recording
them in files that are managed by Parsl.


Limitations
^^^^^^^^^^^

The following limitations apply to Bash apps:

1. Environment variables are not yet supported.

Special Keywords
^^^^^^^^^^^^^^^^

In addition to the ``inputs``, ``outputs``, and ``walltime`` argument keywords described above, a Bash app can take the following keywords:

4. stdout: (string, tuple or `parsl.AUTO_LOGNAME`) The path to a file to which standard output should be redirected. If set to `parsl.AUTO_LOGNAME`, the log will be automatically named according to task id and saved under `task_logs` in the run directory. If set to a tuple `(filename, mode)` then standard output will be redirected to the named file, opened with the specified mode as used by the python `open <https://docs.python.org/3/library/functions.html#open>`_ function.
5. stderr: (string or `parsl.AUTO_LOGNAME`) Like stdout, but for the standard error stream.
6. label: (string) If the app is invoked with `stdout=parsl.AUTO_LOGNAME` or `stderr=parsl.AUTO_LOGNAME`, append `label` to the log name.

A Bash app can construct the string to execute on the command-line from arguments passed
to the decorated function.

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

A Bash app, like a Python app, returns an AppFuture, which the programmer can use to determine when the
app has completed (e.g., via `future.result()` as in the preceding code fragment).
A Bash app can only return results via files specified via ``outputs``, ``stderr``, or ``stdout``  the value returned inside the AppFuture has no real meaning.

If the Bash app exits with Unix exit code 0, then the AppFuture will complete. If the Bash app
exits with any other code, this will be treated as a failure, and the AppFuture will instead
contain an BashExitFailure exception. The Unix exit code can be accessed through the
`exitcode` attribute of that BashExitFailure.
