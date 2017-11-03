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
   3. 


Bash Apps
---------

The Bash app allows you to compose calls to external applications from the commandline as you would in a Bash shell.
This is made possible by defining a python function that sets the special variable ``cmd_line`` to a commandline string.

The following code snippet demonstrates a simple bash script written as a string in Python and wrapped as an App.
The convention here is that of any arbitrarily large string assigned to the variable ``cmd_line`` within an ``@App`` of type `bash`.


.. code-block:: python

       @App('bash', data_flow_kernel)
       def echo_hello(stderr='std.err', stdout='std.out'):
           cmd_line = 'echo "Hello World!"'



The arguments and keyword arguments passed to the function are used to format the command line string once all the arguments
resolved.

.. code-block:: python

       @App('bash', thread_pool_executor)
       def echo(inputs=[], stderr='std.err', stdout='std.out'):
           cmd_line = 'echo {inputs[0]} {inputs[1]}'

