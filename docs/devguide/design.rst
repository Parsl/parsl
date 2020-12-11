Design and Rationale



Swift vs Parsl
--------------

The following text is not well structured, and is mostly a brain dump that needs to be organized.
Moving from Swift to an established language (python) came with its own tradeoffs. We get the backing
of a rich and very well known language to handle the language aspects as well as the libraries.
However, we lose the parallel evaluation of every statement in a script. The thesis is that what we
lose is minimal and will not affect 95% of our workflows. This is not yet substantiated.

Please note that there are two Swift languages: `Swift/K <http://swift-lang.org/main/>`_
and `Swift/T <http://swift-lang.org/Swift-T/index.php>`_ . These have diverged in syntax and behavior.
Swift/K is designed for grids and clusters runs the java based
`Karajan <https://wiki.cogkit.org/wiki/Karajan>`_ (hence, /K) execution framework.
Swift/T is a completely new implementation of Swift/K for high-performance computing. Swift/T uses
Turbine(hence, /T) and and
`ADLB <http://www.mcs.anl.gov/project/adlb-asynchronous-dynamic-load-balancer>`_ runtime libraries for
highly scalable dataflow processing over MPI,
without single-node bottlenecks.


Parallel Evaluation
^^^^^^^^^^^^^^^^^^^

In Swift (K&T), every statement is evaluated in parallel.

.. code-block:: c

    y = f(x);
    z = g(x);

We see that y and z are assigned values in different order when we run Swift multiple times. Swift
evaluates both statements in parallel and the order in which they complete is mostly random.

We will *not* have this behavior in Python. Each statement is evaluated in order.

.. code-block:: c

    int[] array;
    foreach v,i in [1:5] {
       array[i] = 2*v;
    }

    foreach v in array {
       trace(v)
    }

Another consequence is that in Swift, a foreach loop that consumes results in an array need
not wait for the foreach loop that fill the array. In the above example, the second foreach
loop makes progress along with the first foreach loop as it fills the array.

In parsl, a for loop that **launches** tasks has to complete launches before the control may
proceed to the next statement. The first for loop has to simply finish iterating, and launching
jobs, which should take ~length_of_iterable/1000 (items/task_launch_rate).

.. code-block:: python

     futures = {};

     for i in range(0,10):
         futures[i] = app_double(i);

     for i in fut_array:
         print(i, futures[i])

The first for loop first fills the futures dict before control can proceed to the second for
loop that consumes the contents.

The main conclusion here is that, if the iteration space is sufficiently large (or the app
launches are throttled), then it is possible that tasks that are further down the control 
flow have to wait regardless of their dependencies being resolved.


Mappers
^^^^^^^

In Swift/K, a mapper is a mechanism to map files to variables. Swift need's to know files
on disk so that it could move them to remote sites for execution or as inputs to applications.
Mapped file variables also indicate to swift that, when files are created on remote sites, they
need to be staged back. Swift/K provides several mappers which makes it convenient to map files on
disk to file variables.

There are two choices here :

1. Have the user define the mappers and data objects
2. Have the data objects be created only by Apps.


In Swift, the user defines file mappings like this :

.. code-block:: c

     # Mapping a single file
     file f <"f.txt">;

     # Array of files
     file texts[] <filesys_mapper; prefix="foo", suffix=".txt">;

The files mapped to an array could be either inputs or outputs to be created. Which is the case is
inferred from whether they are on the left-hand side or right-hand side of an assignment. Variables on
the left-hand side are inferred to be outputs that have future-like behavior. To avoid conflicting
values being assigned to the same variable, Swift variables are all immutable.

For instance, the following would be a major concern *if* variables were not immutable:

.. code-block:: c

     x = 0;
     x = 1;
     trace(x);

The results that trace would print would be non-deterministic, if x were mutable. In Swift, the above
code would raise an error. However this is perfectly legal in python, and the x would take the last
value it was assigned.

Remote-Execution
^^^^^^^^^^^^^^^^

In Swift/K, remote execution is handled by `coasters <http://swift-lang.org/guides/trunk/userguide/userguide.html#_how_swift_implements_the_site_execution_model>`_.
This is a pilot mechanism that supports dynamic resource provisioning from cluster managers such as PBS,
Slurm, Condor and handles data transport from the client to the workers. Swift/T on the other hand is
designed to run as an MPI job on a single HPC resource. Swift/T utilized shared-filesystems that almost
every HPC resource has.

To be useful, Parsl will need to support remote execution and file transfers. Here we will discuss just
the remote-execution aspect.

Here is a set of features that should be implemented or borrowed :

* [Done] New remote execution system must have the `executor interface <https://docs.python.org/3/library/concurrent.futures.html#executor-objects>`_.
* [Done] Executors must be memory efficient wrt to holding jobs in memory.
* [Done] Continue to support both BashApps and PythonApps.
* [Done] Capable of using templates to submit jobs to Cluster resource managers.
* [Done] Dynamically launch and shutdown workers.

.. note::
   Since the current roadmap to remote execution is through ipython-parallel, we will limit support
   to Python3.5+ to avoid library naming issues.


Availability of Python3.5 on target resources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The availability of Python3.5 on compute resources, especially one's on which the user does not have
admin privileges could be a concern. This was raised by Lincoln from the OSG Team. Here's a small
table of our initial target systems as of Mar 3rd, 2017 :

+----------------------------------------+----------+----------+----------+
| Compute Resource                       |Python3.4 |Python3.5 |Python3.6 |
+========================================+==========+==========+==========+
| Midway (RCC, UChicago)                 |     X    |    X     |          |
+----------------------------------------+----------+----------+----------+
| Open Science Grid                      |     X    |    X     |          |
+----------------------------------------+----------+----------+----------+
| BlueWaters                             |     X    |    X     |          |
+----------------------------------------+----------+----------+----------+
| AWS/Google Cloud                       |     X    |    X     |    X     |
+----------------------------------------+----------+----------+----------+
| Beagle                                 |     X    |          |          |
+----------------------------------------+----------+----------+----------+

======
Design
======

Under construction.

