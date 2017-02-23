Parsl Tutorial
==============

0 to Parsl in 10 minutes.

Parsl library allows you to markup of function for parallel execution.
When a parallel function is invoked a proxy for the results from the function
is returned in the form of ``futures``. ``futures`` are basically a placeholder
for a result from a parallel function that allows you to check the status, get
exceptions in case of errors and get results once they are available. ``futures``
returned by a function can be passed to another parallel function to specify
dependencies.

Parsl provides an ``App`` decorator to markup a function as a parallel function.
Two kinds of ``App`` 's are currently supported : **bash** and **python**.
The ``App`` decorator also accepts an ``executor`` which represents an execution
resource such as threads, processes or even remote workers.

Here's an example using a **bash** app:

.. code-block:: python

    # Import Parsl
    from parsl import *
    import parsl

    # Create a pool of 4 processes
    workers = ProcessPoolExecutor(max_workers=4)

    # Here we define a bash app
    @App('bash', dfk)
    def echo_to_file(inputs=[],             # List of futures or python objects
                     outputs=[],            # List of output files
                     stderr='std.err',      # File to which STDERR is logged
                     stdout='std.out'):     # File to which STDOUT is logged

        # A bash script is assigned to a special variable named cmd_line
        # The string provided is formatted with the args, and kwargs to the fn echo_to_file
        cmd_line = 'echo {inputs} > {outputs[0]}'


    fu, outs = echo_to_file(inputs=["Hello World"], outputs=['hello.txt'])
    fu.wait()

Here's an example of similar behavior with a pure **python** app:

.. code-block:: python

    # Import Parsl
    from parsl import *
    import parsl

    # Create a pool of 4 processes
    workers = ProcessPoolExecutor(max_workers=4)

    # Here we define a bash app
    @App('python', dfk)
    def echo_to_file(string, outputs=[]):
        import os
        with open(outputs[0], 'w') as outfile:
            outfile.write(string)
        return True


    fu, outs = echo_to_file("Hello World", outputs=['hello.txt'])
    fu.wait()


Dependencies
------------

The above example shows you how to define app 
