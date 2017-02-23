Apps
====

An app is a piece of code that executes independently on an execution resource. An execution resource in this context can be a pool of `threads <https://en.wikipedia.org/wiki/Thread_(computing)>`_, `processes <https://en.wikipedia.org/wiki/Process_(computing)>`_ or even remote workers.

Parsl allows you to markup existing python functions or even snippets of bash script as Apps using the ``@App`` decorator. We currently support pure python functions and bash scripts as Apps. Here's are a couple examples :


The following code snippet shows you a simple python function ``double(Int)`` that has been converted to an App using the ``@App`` decorator. Note that the first argument to ``@App`` specifies the App type as python. It is important to note that decorated functions should be pure functions that only act on the input args, and must also explicitly import any modules used.

.. code-block:: python

       @App('python', thread_pool_executor)
       def double(x):
           return x*2

The following code snippet demonstrates a simple bash script written as a string in Python and wrapped as an App. The convention here is that any arbitrarily large string assigned to the variable ``cmd_line`` within an ``@App`` of type `bash`

.. code-block:: python

       @App('bash', process_pool_executor)
       def echo(inputs=[], stderr='std.err', stdout='std.out'):
           cmd_line = 'echo {inputs[0]} {inputs[1]}'
