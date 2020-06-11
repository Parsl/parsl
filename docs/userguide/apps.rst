Apps
====

An **app** is a Parsl construct for representing a fragment of Python code 
or external Bash shell code that can be asynchronously executed.

A Parsl app is defined by annotating a Python function with a decorator: 
the ``@python_app`` decorator for a **Python app**, and the ``@bash_app`` decorator for a **Bash app**. 
Python apps encapsulate pure Python code, while Bash apps wrap calls to external applications and scripts.

Python Apps
-----------

The following code snippet shows a Python function ``double(x: int)``, which returns double the input
value. 
The ``@python_app`` decorator defines the function as a Parsl Python app.  

.. code-block:: python

       @python_app
       def double(x):
           return x * 2

       double(42)

As a Parsl Python app is executed asynchronously, and potentially remotely, the function
cannot assume access to shared program state. For example, it must explicitly import any 
required modules and cannot refer to variables used outside the function. 
Thus while the following code fragment is valid Python, it is not valid Parsl, 
as the `bad_double()` function requires the `random` module and refers to the external 
variable `factor`.

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

Python apps may be passed any Python input argument, including primitive types, 
files, and other complex types that can be serialized (e.g., numpy array,
scikit-learn model). They may also be passed a Parsl `Future` (see :ref:`label-futures`) 
returned by another Parsl app.
In this case, Parsl will establish a dependency between the two apps and will not 
execute the dependent app until all dependent futures are resolved.
Further detail is provided in :ref:`label-futures`.

A Python app may also act upon files. In order to make Parsl aware of these files, they must be specified by using the ``inputs`` and/or ``outputs`` keyword arguments, as in following code snippet, which copies the contents of one file (`in.txt`) to another (`out.txt`).

.. code-block:: python

       @python_app
       def echo(inputs=[], outputs=[]):
           with open(inputs[0], 'r') as in_file, open(outputs[0], 'w') as out_file:
               out_file.write(in_file.readline())

       echo(inputs=[in.txt], outputs=[out.txt])

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
3. walltime: (int) This keyword argument places a limit on the app's
   runtime in seconds. If the walltime is exceed, Parsl will raise an `parsl.app.errors.AppTimeout` exception.

Returns
^^^^^^^

A Python app returns an AppFuture (see :ref:`label-futures`) as a proxy for the results that will be returned by the
app once it is executed. This future can be inspected to obtain task status; 
and it can be used to wait for the result, and when complete, present the output Python object(s) returned by the app.
In case of an error or app failure, the future holds the exception raised by the app.

Limitations
^^^^^^^^^^^

There are some limitations on the Python functions that can be converted to apps:

1. Functions should act only on defined input arguments. That is, they should not use script-level or global variables.
2. Functions must explicitly import any required modules.
3. Parsl uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ and pickle to serialize Python objects to/from apps. Therefore, Parsl require that all input and output objects can be serialized by cloudpickle or pickle. See :ref:`label_serialization_error`.
4. STDOUT and STDERR produced by Python apps remotely are not captured.


Bash Apps
---------

A Parsl Bash app is used to execute an external application, script, or code written in another language.
It is defined by a ``@bash_app`` decorator and the Python code that forms the body of the
function must return a fragment of Bash shell code to be executed by Parsl.
The Bash shell code executed by a Bash app can be arbitrarily long. 

The following code snippet presents an example of a Bash app `echo_hello`,
which returns the bash command `'echo "Hello World!"'` as a string. 
This string will be executed by Parsl as a Bash command.

.. code-block:: python

       @bash_app
       def echo_hello(stderr='std.err', stdout='std.out'):
           return 'echo "Hello World!"'

       # echo_hello() when called will execute the shell command and
       # create a std.out file with the contents "Hello World!"
       echo_hello()


Unlike a Python app, a Bash app cannot return Python objects.
Instead, Bash apps communicate with other apps via files.
A decorated ``@bash_app`` exposes the ``inputs`` and ``outputs`` keyword arguments 
described above for tracking input and output files.
It also includes, as described below, keyword arguments for capturing the STDOUT and STDERR streams and recording
them in files that are managed by Parsl.

Special Keywords
^^^^^^^^^^^^^^^^

In addition to the ``inputs``, ``outputs``, and ``walltime`` keyword arguments
described above, a Bash app can accept the following keywords:

1. stdout: (string, tuple or `parsl.AUTO_LOGNAME`) The path to a file to which standard output should be redirected. If set to `parsl.AUTO_LOGNAME`, the log will be automatically named according to task id and saved under `task_logs` in the run directory. If set to a tuple `(filename, mode)`, standard output will be redirected to the named file, opened with the specified mode as used by the Python `open <https://docs.python.org/3/library/functions.html#open>`_ function.
2. stderr: (string or `parsl.AUTO_LOGNAME`) Like stdout, but for the standard error stream.
3. label: (string) If the app is invoked with `stdout=parsl.AUTO_LOGNAME` or `stderr=parsl.AUTO_LOGNAME`, this arugment will be appended to the log name.

A Bash app can construct the Bash command string to be executed from arguments passed
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

A Bash app, like a Python app, returns an AppFuture, which can be used to obtain
task status, determine when the app has completed (e.g., via `future.result()` as in the preceding code fragment), and access exceptions.
As a Bash app can only return results via files specified via ``outputs``, ``stderr``, or ``stdout``; the value returned by the AppFuture has no meaning.

If the Bash app exits with Unix exit code 0, then the AppFuture will complete. If the Bash app
exits with any other code, Parsl will treat this as a failure, and the AppFuture will instead
contain an `BashExitFailure` exception. The Unix exit code can be accessed through the
`exitcode` attribute of that `BashExitFailure`.

Limitations
^^^^^^^^^^^

The following limitation applies to Bash apps:

1. Environment variables are not supported.
