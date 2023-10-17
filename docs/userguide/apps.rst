.. _apps:

Apps
====

An **App** defines a computation that will be executed asynchronously by Parsl.
Apps are Python functions marked with a decorator which
designates that the function will run asynchronously and cause it return
a :class:`~concurrent.futures.Future` instead of the result.

Apps can be one of three types of functions, each with their own type of decorator

- ``@python_app``: Most Python functions
- ``@bash_app``: A python function which returns a command line program to execute
- ``@join_app``: A function which returns launches other apps

The intricacies of Python and Bash apps are documented below. Join apps are documented in a later
section (see :ref:`label-joinapp`).

Python Apps
-----------

.. code-block:: python

    @python_app
    def hello_world(name: str) -> str:
        return f'Hello, {name}!'

    print(hello_world('user').result())


Python Apps run Python functions. The code inside a function marked by ``@python_app`` will run on a remote system.

Most functions can run without modification.
Limitations on the content of the functions and their inputs/outputs are described below.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _function-rules:

Parsl apps have access to less information from the script that defined them
than functions run via Python's native multiprocessing libraries.
The reason is that functions are executed on workers that
lack access to the global variables in the script that defined them.
Practically, this means

1. *Functions may need to re-import libraries.*
   Place the import statements that define functions or classes inside the function.
   Type annotations should not use libraries defined in the function.


  .. code-block:: python

    import numpy as np

    # BAD: Assumes library has been imported
    @python_app
    def linear_model(x: list[float] | np.ndarray, m: float, b: float):
        return np.multiply(x, m) + b

    # GOOD: Function imports libraries on remote worker
    @python_app
    def linear_model(x: list[float] | 'np.ndarray', m: float, b: float):
        import numpy as np
        return np.multiply(x, m) + b


2. *Global variables are inaccessible*.
   Functions should not use variables defined outside the function.
   Likewise, do not assume that variables created inside the function are visible elsewhere.


.. code-block:: python

    # BAD: Uses global variables
    global_var = {'a': 0}

    @python_app
    def counter_func(string: str, character: str = 'a'):
        global_var[character] += string.count(character)  # `global_var` will not be accessible


    # GOOD
    @python_app
    def counter_func(string: str, character: str = 'a'):
        return {character: string.count(character)}

    for ch, co in good_global('parsl', 'a').result().items():
        global_var[ch] += co


Functions from Modules
++++++++++++++++++++++

The above rules do not apply for functions defined in Python modules.
Functions in installed modules are sent to workers differently than functions defined in a script.

Supply a module function as an argument to ``python_app`` rather than creating a new function which is decorated.

.. code-block:: python

    from module import function
    function_app = python_app(function, executors='all')

``function_app`` will act as Parsl App function of ``function``.

It is also possible to create wrapped versions of functions, such as ones with pinned arguments.
Parsl just requires first calling :meth:`~functools.update_wrapped` with the wrapped function
to include attributes from the original function (e.g., its name).

.. code-block:: python

    from functools import partial, update_wrapped
    import numpy as np
    my_max = partial(np.max, axis=0, keepdims=True)
    my_max = update_wrapper(my_max, max)  # Copy over the names
    my_max_app = python_app(my_max)



Inputs and Outputs
^^^^^^^^^^^^^^^^^^

Python apps may be passed any Python type as an input and return any Python type, with a few exceptions.
There are several classes of allowed types, each with different rules.

- *Python Objects*: Any Python object that can be saved with
  `pickle <https://docs.python.org/3/library/pickle.html>`_ or `dill <https://dill.readthedocs.io/>`_
  can be used as an import or output.
  All primitive types (e.g., floats, strings) are valid as are many complex types (e.g., numpy arrays).
- *Files*: Pass files as inputs as a :py:class:`~parsl.data_provider.files.File` object.
  Parsl can transfer them to a remote system and update the ``File`` object with a new path.
  Access the new path with ``File.filepath`` attribute.

  .. code-block:: python

      @python_app
      def read_first_line(x: File):
          with open(x.filepath, 'r') as fp:
              return fp.readline()

  Files can also be outputs of a function, but only through the ``outputs`` kwargs (described below).
- *Parsl Futures*. Functions can receive results from other Apps as Parsl ``Future``s objects.
  Parsl will establish a dependency on the App(s) which created the Future(s)
  and start executing as soon as the preceding ones complete.

  .. code-block:: python

    @python_app
    def capitalize(x: str):
        return x.upper()

    input_file = File('text.txt')
    first_line_future = read_first_line(input_file)
    capital_future = capitalize(first_line_future)
    print(capital_future.result())

  See the section on `Futures <futures.html>`_ for more details.


Learn more about the types of data allowed in `the data section <data.html>`_.

Special Keyword Arguments
+++++++++++++++++++++++++

Parsl apps can use the following special reserved keyword arguments:

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

Outputs
+++++++

A Python app returns an AppFuture (see :ref:`label-futures`) as a proxy for the results that will be returned by the
app once it is executed. This future can be inspected to obtain task status; 
and it can be used to wait for the result, and when complete, present the output Python object(s) returned by the app.
In case of an error or app failure, the future holds the exception raised by the app.

Limitations
^^^^^^^^^^^

To summarize, any Python function can be made a Python App with a few restrictions

1. Functions should act only on defined input arguments. That is, they should not use script-level or global variables.
2. Functions must explicitly import any required modules if they are defined in script which starts Parsl.
3. Parsl uses dill and pickle to serialize Python objects to/from apps. Therefore, Parsl require that all input and output objects can be serialized by dill or pickle. See :ref:`label_serialization_error`.
4. STDOUT and STDERR produced by Python apps remotely are not captured.


Bash Apps
---------

.. code-block:: python

       @bash_app
       def echo(
           name: str,
           stdout=parsl.AUTO_LOGNAME  # Requests Parsl to return the stdout
       ):
           return f'echo "Hello, {name}!"'

       future = echo('user')
       future.result() # block until task has completed

       with open(future.stdout, 'r') as f:
           print(f.read())


A Parsl Bash app executes an external application by making a command-line execution.
Parsl will execute the string returned by the function as a command-line script on a remote worker.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Bash Apps follow the same rules :ref:`as Python Apps <function-rules>`.
For example, imports may need to be inside functions and global variables will be inaccessible.

Inputs and Outputs
^^^^^^^^^^^^^^^^^^

Bash Apps can use the same kinds of inputs as Python Apps, but only communicate results with Files.

The Bash Apps, unlike Python Apps, can also return the content printed to the Standard Output and Error.

Special Keywords Arguments
+++++++++++++++++++++++++

In addition to the ``inputs``, ``outputs``, and ``walltime`` keyword arguments
described above, a Bash app can accept the following keywords:

1. stdout: (string, tuple or ``parsl.AUTO_LOGNAME``) The path to a file to which standard output should be redirected. If set to ``parsl.AUTO_LOGNAME``, the log will be automatically named according to task id and saved under ``task_logs`` in the run directory. If set to a tuple ``(filename, mode)``, standard output will be redirected to the named file, opened with the specified mode as used by the Python `open <https://docs.python.org/3/library/functions.html#open>`_ function.
2. stderr: (string or ``parsl.AUTO_LOGNAME``) Like stdout, but for the standard error stream.
3. label: (string) If the app is invoked with ``stdout=parsl.AUTO_LOGNAME`` or ``stderr=parsl.AUTO_LOGNAME``, this argument will be appended to the log name.

Outputs
+++++++

If the Bash app exits with Unix exit code 0, then the AppFuture will complete. If the Bash app
exits with any other code, Parsl will treat this as a failure, and the AppFuture will instead
contain an `BashExitFailure` exception. The Unix exit code can be accessed through the
``exitcode`` attribute of that `BashExitFailure`.

Limitations
^^^^^^^^^^^

The following limitation applies to Bash apps:

1. Environment variables are not supported.


MPI Apps
^^^^^^^^

Applications which employ MPI to span multiple nodes are a special case of Bash apps,
and require special modification of Parsl's `execution environment <execution.html>`_ to function.
Support for MPI applications is described `in a later section <mpi_apps.html>`_.
