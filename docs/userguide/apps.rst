Apps
====

An app is a piece of code that executes asynchronously on an execution resource.
An execution resource in this context can be a pool of `threads <https://en.wikipedia.org/wiki/Thread_(computing)>`_, `processes <https://en.wikipedia.org/wiki/Process_(computing)>`_, or even remote workers.

Parsl allows you to markup existing python functions or even snippets of bash script as Apps using the ``@App`` decorator.
We currently support pure python functions by specifying the app type as ``python`` and calls to external application through bash scripts using app type ``bash``.

Python Apps
-----------

The following code snippet shows a simple python function ``double(Int)`` that has been converted to an App using the ``@App`` decorator.
Note that the first argument to ``@App`` specifies the App type as python. It is important to note that decorated functions should be pure
functions that only act on the input args, and must also explicitly import any modules used.

.. code-block:: python

       from parsl import *
       workers = ThreadPoolExecutor(max_workers=4)
       dfk = DataFlowKernel(executors=[workers])

       @App('python', data_flow_kernel)
       def double(x):
             return x*2

       double(x)

Limitations
^^^^^^^^^^^

There are limitations on what functions could be converted to apps:

1. Functions should only act only on the inputs
2. Functions should not rely on side-effects such as global variables
3. Parsl uses `cloudpickle<https://github.com/cloudpipe/cloudpickle>`_ and pickle to serialize Python constructs,
   such as inputs and outputs to functions. Therefore, Functions can only use inputs and outputs that can be
   serialized by cloudpickle or pickle.

Special Keywords
^^^^^^^^^^^^^^^^

Any python function decorated with the ``@App`` decorator can take a few special reserved keyword arguments.

1. inputs : (list) This keyword argument allows you to pass a list of :ref:`label-futures`, and thus wait on
   the results from a list of ``Apps``.
2. walltime :(int) This keyword argument is used to specify the duration in seconds for which the function is
   allowed to run. This keyword will be used in the future, but is not yet enabled.

Bash Apps
---------

The Bash app allows you to compose calls to external applications from the command-line as you would in a Bash shell.
This is made possible by defining a python function that returns the command-line string that is to be executed.

The following code snippet demonstrates a simple bash script written as a string in Python and wrapped as an App.
Any command-line invocation represented by an arbitrarily large string, can be returned by a function decorated
within an ``@App`` of type `bash` to be executed. Since most unix tools use files as input and outputs, the
decorated `bash` function supports a few special keyword arguments to support files and other needs.


.. code-block:: python

       @App('bash', data_flow_kernel)
       def echo_hello(stderr='std.err', stdout='std.out'):
           return 'echo "Hello World!"'

       # echo_hello() when called will execute the string it returns, creating an std.out file with
       # the contents "Hello World!"
       echo_hello()


As shown above, special keyword arguments ``stdout`` and ``stderr`` passed to a bash app function
allow for the capture of the STDOUT and STDERR streams to specific files. The set of special
keywords that maybe used are listed below :

Special Keywords
^^^^^^^^^^^^^^^^

1. inputs : (list) This keyword argument, just like in python apps is used to pass a list of :ref:`label-futures`,
   and thus wait on the results from a list of ``Apps``.
2. outputs : (list) List of filenames that will be created by the app. This is required so parsl can check
   if they were created correctly, track them and even move them when being executed on remote machines.
3. stdout : (string) Specify the filepath to a file to which STDOUT should be redirected.
4. stderr : (string) Specify the filepath to a file to which STDERR should be redirected.
5. walltime :(int) This keyword argument is used to specify the duration in seconds for which the function is
   allowed to run. An AppTimeout exception is raised if walltime is exceeded.

The Bash app allows a user to compose the string to execute on the command-line from the various arguments passed
to the decorated function. The string that is returned is formatted by the python string `format <https://docs.python.org/3.4/library/functions.html#format>`_  (`PEP 3101 <https://www.python.org/dev/peps/pep-3101/>`_).

.. code-block:: python

       @App('bash', thread_pool_executor)
       def echo(arg1, inputs=[], stderr='std.err', stdout='std.out'):
           return 'echo {0} {inputs[0]} {inputs[1]}'

       # This call echoes "Hello World !" to the file *std.out*
       echo("Hello", inputs=["World", "!"])
