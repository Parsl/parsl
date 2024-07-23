.. _apps:

Apps
====

An **App** is a piece of code that will run asynchronously with Parsl. Apps are Python functions marked with a special decorator that makes them run asynchronously. Instead of returning a normal result, these functions return a `concurrent.futures.Future`.

There are three types of App functions, each with its own decorator:

- ``@python_app``: Most Python functions
- ``@bash_app``: A Python function that runs a command line program
- ``@join_app``: A function that starts one or more new Apps

Below, we explain Python and Bash Apps. Join Apps are explained later (see `Join Apps <label-joinapp>`_).

Python Apps
-----------

.. code-block:: python

    @python_app
    def hello_world(name: str) -> str:
        return f'Hello, {name}!'

    print(hello_world('user').result())

Python Apps run Python functions. When a function has the ``@python_app`` decorator, it can run either locally or on a remote system.

Most functions work without changes. However, there are some rules about what the functions can and cannot do.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Parsl Apps don't have access to the global variables of the script that defines them. This is different from Python's native multiprocessing. Here are the important rules:

1. **Re-import Libraries Inside Functions**:
   If your function uses libraries, you must import them inside the function.

  .. code-block:: python

    import numpy as np

    # BAD: Assumes library is already imported
    @python_app
    def linear_model(x: list[float] | np.ndarray, m: float, b: float):
        return np.multiply(x, m) + b

    # GOOD: Imports library inside the function
    @python_app
    def linear_model(x: list[float] | 'np.ndarray', m: float, b: float):
        import numpy as np
        return np.multiply(x, m) + b

2. **Don't Use Global Variables**:
   Functions should not use variables defined outside of them. Variables created inside the function are also not visible outside of it.

  .. code-block:: python

    # BAD: Uses a global variable
    global_var = {'a': 0}

    @python_app
    def counter_func(string: str, character: str = 'a'):
        global_var[character] += string.count(character)  # `global_var` won't be accessible

    # GOOD: Uses only local variables
    @python_app
    def counter_func(string: str, character: str = 'a'):
        return {character: string.count(character)}

    for ch, co in counter_func('parsl', 'a').result().items():
        global_var[ch] += co

3. **Use Return Statements for Outputs**:
   Parsl does not support generator functions (functions that use ``yield``) and changes to input arguments won't be communicated.

  .. code-block:: python

    # BAD: Assumes changes to inputs will be communicated
    @python_app
    def append_to_list(input_list: list, new_val):
        input_list.append(new_val)

    # GOOD: Returns the modified list
    @python_app
    def append_to_list(input_list: list, new_val) -> list:
        input_list.append(new_val)
        return input_list

Functions from Modules
++++++++++++++++++++++

If you are using functions from an installed Python module, the rules above do not apply. Functions from modules are handled differently.

You can convert a function from a library to a Python App by passing it to ``python_app``:

.. code-block:: python

    from module import function
    function_app = python_app(function)

``function_app`` will work as a Parsl App function.

You can also create wrapped versions of functions, like ones with specific arguments. Use `functools.update_wrapper` to keep the function's attributes (like its name).

  .. code-block:: python

    from functools import partial, update_wrapper
    import numpy as np
    my_max = partial(np.max, axis=0, keepdims=True)
    my_max = update_wrapper(my_max, np.max)  # Copy over the names
    my_max_app = python_app(my_max)

This example is the same as creating a new function:

  .. code-block:: python

    @python_app
    def my_max_app(*args, **kwargs):
        import numpy as np
        return np.max(*args, keepdims=True, axis=0, **kwargs)

Inputs and Outputs
^^^^^^^^^^^^^^^^^^

Python Apps can take and return any Python type, with a few exceptions.

- **Python Objects**: Any object that can be saved with `pickle <https://docs.python.org/3/library/pickle.html>`_ or `dill <https://dill.readthedocs.io/>`_ can be used. This includes primitive types like floats and strings, as well as more complex types like numpy arrays.
- **Files**: Use `parsl.data_provider.files.File` objects to pass files. Parsl can transfer them to a remote system and update the ``File`` object with the new path.

  .. code-block:: python

      @python_app
      def read_first_line(x: File):
          with open(x.filepath, 'r') as fp:
              return fp.readline()

  Files can also be outputs, but only through the ``outputs`` keyword argument.
- **Parsl Futures**: Functions can take results from other Apps as Parsl ``Future`` objects. Parsl will wait for these results before running the function.

  .. code-block:: python

    @python_app
    def capitalize(x: str):
        return x.upper()

    input_file = File('text.txt')
    first_line_future = read_first_line(input_file)
    capital_future = capitalize(first_line_future)
    print(capital_future.result())

Learn more about data types in the `data section <data.html>`_.

.. note::

    Changes to mutable input arguments will be ignored.

Special Keyword Arguments
+++++++++++++++++++++++++

Some keyword arguments are treated differently by Parsl:

1. **inputs**: (list) A list of input futures or files. Parsl will wait for these to resolve before running the app.

  .. code-block:: python

    @python_app
    def map_app(x):
        return x * 2

    @python_app
    def reduce_app(inputs=()):
        return sum(inputs)

    map_futures = [map_app(x) for x in range(3)]
    reduce_future = reduce_app(inputs=map_futures)

    print(reduce_future.result())  # 0 + 1*2 + 2*2 = 6

2. **outputs**: (list) A list of files that will be produced by the app. Parsl will track these files and ensure they are correctly created.

  .. code-block:: python

    @python_app
    def write_app(message, outputs=()):
        """Write a single message to every file in outputs"""
        for path in outputs:
            with open(path, 'w') as fp:
                print(message, file=fp)

    to_write = [
        File('output-0.txt'),
        File('output-1.txt')
    ]
    write_app('Hello!', outputs=to_write).result()
    for path in to_write:
        with open(path) as fp:
            assert fp.read() == 'Hello!\n'

3. **walltime**: (int) The time limit for the app's runtime in seconds. If the walltime is exceeded, Parsl will raise a `parsl.app.errors.AppTimeout` exception.

Outputs
+++++++

A Python App returns an `AppFuture <label-futures>`_, which is a proxy for the results. The future can be checked for status, waited on for the result, and will hold the output when complete. If there is an error, the future will hold the exception raised by the app.

Options for Python Apps
^^^^^^^^^^^^^^^^^^^^^^^

The `parsl.app.app.python_app` decorator has options to control how Parsl runs tasks. For example, you can cache results or run tasks on specific sites.

  .. code-block:: python

    @python_app(cache=True, executors=['gpu'])
    def expensive_gpu_function():
        # ...
        return

See the Parsl documentation for more details.

Limitations
^^^^^^^^^^^

To summarize, any Python function can be a Python App with these restrictions:

1. Functions should only use defined input arguments, not script-level or global variables.
2. Functions must import any required modules if they are defined in the script that starts Parsl.
3. Parsl requires that all input and output objects can be serialized by dill or pickle. See `Serialization Error <label_serialization_error>`_.
4. STDOUT and STDERR produced by Python Apps remotely are not captured.

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
       future.result()  # block until task has completed

       with open(future.stdout, 'r') as f:
           print(f.read())

A Parsl Bash App runs an external application by executing a command-line script on a remote worker.

Rules for Function Contents
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Bash Apps follow the same rules as Python Apps. For example, imports must be inside functions, and global variables are not accessible.

Inputs and Outputs
^^^^^^^^^^^^^^^^^^

Bash Apps use the same kinds of inputs as Python Apps, but can only communicate results with Files. Unlike Python Apps, Bash Apps can also return content printed to Standard Output and Error.

Special Keyword Arguments
+++++++++++++++++++++++++

In addition to ``inputs``, ``outputs``, and ``walltime`` keyword arguments, a Bash App can accept these:

1. **stdout**: (string, tuple, or ``parsl.AUTO_LOGNAME``) The path to a file for standard output. If set to ``parsl.AUTO_LOGNAME``, the log will be automatically named according to task ID and saved under ``task_logs`` in the run directory. If set to a tuple ``(filename, mode)``, standard output will be redirected to the named file, opened with the specified mode.
2. **stderr**: (string or ``parsl.AUTO_LOGNAME``) Like stdout, but for standard error.
3. **label**: (string) If ``stdout=parsl.AUTO_LOGNAME`` or ``stderr=parsl.AUTO_LOGNAME``, this argument is appended to the log name.

Outputs
+++++++

If the Bash App exits with Unix exit code 0, the AppFuture will complete. If it exits with any other code, Parsl will treat it as a failure, and the AppFuture will contain a `BashExitFailure` exception. The Unix exit code can be accessed through the ``exitcode`` attribute of `BashExitFailure`.

Execution Options
^^^^^^^^^^^^^^^^^

Bash Apps have the same execution options (like running on specific sites) as Python Apps.

MPI Apps
^^^^^^^^

MPI applications, which use MPI to run on multiple nodes, are a special case of Bash Apps. They require special configuration in Parsl's `execution environment <execution.html>`_. Support for MPI applications is described `in a later section <mpi_apps.html>`_.
