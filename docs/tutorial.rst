Parsl Tutorial
--------------

Parsl allows you to write functions that execute in parallel and tie
them together with dependencies to create workflows in python. Parsl
wraps python functions into Apps with the **@App** decorator. Decorated
function can run in parallel when all their inputs are ready.

For a deeper dive into examples and documentation, please refer to our
documentation `here <parsl.readthedocs.io>`__.

.. code:: ipython3

    # Import Parsl
    import parsl
    from parsl import *

Parsl's DataFlowKernel acts as a layer over any pool of execution
resources, in our case a pool of
`threads <https://en.wikipedia.org/wiki/Thread\_(computing)>`__.

.. code:: ipython3

    # Let's create a pool of threads to execute our functions
    workers = ThreadPoolExecutor(max_workers=4)
    # We pass the workers to the DataFlowKernel which will execute our Apps over the workers.
    dfk = DataFlowKernel(executors=[workers])

Hello World App
~~~~~~~~~~~~~~~

Let's define a simple python function that returns the string 'Hello
World!'. This function is made an App using the **@App** decorator. The
decorator itself takes the type of app ('python'\|'bash') and the
DataFlowKernel object as arguments.

.. code:: ipython3

    # Here we define our first App function, a simple python app that returns a string
    @App('python', dfk)
    def hello ():
        return 'Hello World!'

    app_future = hello()

Futures
~~~~~~~

Unlike a regular python function, when an App is called it returns an
AppFuture.
`Futures <https://en.wikipedia.org/wiki/Futures_and_promises>`__ act as
a proxy to the results or exceptions that the App will produce once its
execution completes. You can ask a future object its status with
future.done() or ask it to wait for its result with the result() call.
It is important to note that while the done() call just gives you the
current status, the result() call blocks execution till the App is
complete and the result is available.

.. code:: ipython3

    # Check status
    print("Status: ", app_future.done())

    # Get result
    print("Result: ", app_future.result())

Data Dependencies
~~~~~~~~~~~~~~~~~

When a future created by an App is passed as input to another, a data
dependency is created. Parsl ensures that Apps are executed as their
dependencies are resolved.

Let's see an example of this using the `monte-carlo
method <https://en.wikipedia.org/wiki/Monte_Carlo_method#History>`__ to
calculate pi. We call 3 iterations of this slow function, and take the
average. The dependency chain looks like this :

::

    App Calls    pi()  pi()   pi()
                  \     |     /
    Futures        a    b    c
                    \   |   /
    App Call         mysum()
                        |
    Future            avg_pi

.. code:: ipython3

    @App('python', dfk)
    def pi(total):
        import random      # App functions have to import modules they will use.
        width = 10000      # Set the size of the box in which we drop random points
        center = width/2
        c2  = center**2
        count = 0
        for i in range(total):
            # Drop a random point in the box.
            x,y = random.randint(1, width),random.randint(1, width)
            # Count points within the circle
            if (x-center)**2 + (y-center)**2 < c2:
                count += 1
        return (count*4/total)

    @App('python', dfk)
    def mysum(a,b,c):
        return (a+b+c)/3

Parallelism
~~~~~~~~~~~

Here we call the function **pi()** three times, each of which run
independently in parallel. We then call the next app **mysum()** with
the three app futures that were returned from the **pi()** calls. Since
**mysum()** is also a parsl app, it returns an app future immediately,
but defers execution (blocks) until all the futures passed to it as
inputs have resolved.

.. code:: ipython3

    a, b, c = pi(10**6), pi(10**6), pi(10**6)
    avg_pi  = mysum(a, b, c)

.. code:: ipython3

    # Print the results
    print("A: {0:5} B: {1:5} B: {2:5}".format(a.result(), b.result(), c.result()))
    print("Average: {0:5}".format(avg_pi.result()))

Bash Apps
~~~~~~~~~

Science aplications often use external software that are invoked from
the command line. For instance parameter sweeps with molecular dynamics
software such as `LAMMPS <http://lammps.sandia.gov/>`__ are very common.
Next we will see a simple mocked up science workflow composed of bash
apps.

In a bash app function, there are a few special reserved keyword
arguments:

-  inputs (List) : A list of strings or DataFutures
-  outputs (List) : A list of output file paths
-  stdout (str) : redirects STDOUT to string filename
-  stderr (str) : redirects STDERR to string filename

In addition if a list of output filenames are provided via the
outputs=[], a list of DataFutures corresponding to each filename in the
outputs list is made available via the `outputs` attribute of the AppFuture.

.. code:: ipython3

    @App('bash', dfk)
    def sim_mol_dyn(i, dur, outputs=[], stdout=None, stderr=None):
        # The bash app function composes a commandline invocations as a string of arbitrary length
        # that is returned by the function. Positional and Keyword args to the fn() are formatted
        # into the returned string
        return """echo "{0}" > {outputs[0]}
        sleep {1};
        ls ;
        """
    # We call sim_mol_dyn with
    sim_fut = sim_mol_dyn(5, 3, outputs=['sim.out'], stdout='stdout.txt', stderr='stderr.txt')

    data_futs = sim_fut.outputs

.. code:: ipython3

    print(sim_fut, data_futs)
