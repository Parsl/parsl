Python Apps
-----------

.. code-block:: python

    @python_app
    def hello_world(name: str) -> str:
        return f'Hello, {name}!'

    print(hello_world('user').result())


Python Apps run Python functions. The code inside a function marked by ``@python_app`` is what will
be executed either locally or on a remote system.

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



3. *Outputs are only available through return statements*.
   Parsl does not support generator functions (i.e., those which use ``yield`` statements) and
   any changes to input arguments will not be communicated.

.. code-block:: python

    # BAD: Assumes changes to inputs will be communicated
    @python_app
    def append_to_list(input_list: list, new_val):
        input_list.append(new_val)


    # GOOD: Changes to inputs are returned
    @python_app
    def append_to_list(input_list: list, new_val) -> list:
        input_list.append(new_val)
        return input_list


.. _functions-from-modules:

Functions from Modules
++++++++++++++++++++++

The above rules assume that the user is running the example code from a standalone script or Jupyter Notebook.
Functions that are defined in an installed Python module do not need to abide by these guidelines,
as they are sent to workers differently than functions defined locally within a script.

Directly convert a function from a library to a Python App by passing it as an argument to ``python_app``:

.. code-block:: python

    from module import function
    function_app = python_app(function)

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

The above example is equivalent to creating a new function (as below)

.. code-block:: python

    @python_app
    def my_max_app(*args, **kwargs):
        import numpy as np
        return np.max(*args, keepdims=True, axis=0, **kwargs)

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
- *Parsl Futures*. Functions can receive results from other Apps as Parsl ``Future`` objects.
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

  See the section on `Futures <../workflows/futures.html>`_ for more details.


Learn more about the types of data allowed in `the data section <../configuration/data.html>`_.

.. note::

    Any changes to mutable input arguments will be ignored.

.. _special-kwargs:

Special Keyword Arguments
+++++++++++++++++++++++++

Some keyword arguments to the Python function are treated differently by Parsl

1. inputs: (list) This keyword argument defines a list of input :ref:`label-futures` or files.
   Parsl will wait for the results of any listed :ref:`label-futures` to be resolved before executing the app.
   The ``inputs`` argument is useful both for passing files as arguments
   and when one wishes to pass in an arbitrary number of futures at call time.

.. code-block:: python

    @python_app()
    def map_app(x):
        return x * 2

    @python_app()
    def reduce_app(inputs = ()):
        return sum(inputs)

    map_futures = [map_app(x) for x in range(3)]
    reduce_future = reduce_app(inputs=map_futures)

    print(reduce_future.result())  # 0 + 1 * 2 + 2 * 2 = 6

2. outputs: (list) This keyword argument defines a list of files that
   will be produced by the app. For each file thus listed, Parsl will create a future,
   track the file, and ensure that it is correctly created. The future
   can then be passed to other apps as an input argument.

.. code-block:: python

    @python_app()
    def write_app(message, outputs=()):
        """Write a single message to every file in outputs"""
        for path in outputs:
            with open(path, 'w') as fp:
                print(message, file=fp)

    to_write = [
        File(Path(tmpdir) / 'output-0.txt'),
        File(Path(tmpdir) / 'output-1.txt')
    ]
    write_app('Hello!', outputs=to_write).result()
    for path in to_write:
        with open(path) as fp:
            assert fp.read() == 'Hello!\n'

3. walltime: (int) This keyword argument places a limit on the app's
   runtime in seconds. If the walltime is exceed, Parsl will raise an `parsl.app.errors.AppTimeout` exception.

Outputs
+++++++

A Python app returns an AppFuture (see :ref:`label-futures`) as a proxy for the results that will be returned by the
app once it is executed. This future can be inspected to obtain task status;
and it can be used to wait for the result, and when complete, present the output Python object(s) returned by the app.
In case of an error or app failure, the future holds the exception raised by the app.

Options for Python Apps
^^^^^^^^^^^^^^^^^^^^^^^

The :meth:`~parsl.app.app.python_app` decorator has a few options which controls how Parsl executes all tasks
run with that application.
For example, you can ensure that Parsl caches the results of the function and executes tasks on specific sites.

.. code-block:: python

    @python_app(cache=True, executors=['gpu'])
    def expensive_gpu_function():
        # ...
        return

See the Parsl documentation for full details.

Limitations
^^^^^^^^^^^

To summarize, any Python function can be made a Python App with a few restrictions

1. Functions should act only on defined input arguments. That is, they should not use script-level or global variables.
2. Functions must explicitly import any required modules if they are defined in script which starts Parsl.
3. Parsl uses dill and pickle to serialize Python objects to/from apps. Therefore, Parsl require that all input and output objects can be serialized by dill or pickle. See :ref:`label_serialization_error`.
4. STDOUT and STDERR produced by Python apps remotely are not captured.
