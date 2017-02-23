Design and Rationale
====================



Swift vs Parsl
--------------

The following text is not well structured, and is mostly a brain dump that needs to be organized.
Moving from swift to an established language (python) came with it's own tradeoffs. We get the backing
of a rich and very well known language to handle the language aspects as well as the libraries.
However, we lose the parallel evaluation of every statement in a statement. The thesis is that what we
lose is minimal and will not affect 95% of our workflows. This is not yet substanciated.

Parallel Evaluation
^^^^^^^^^^^^^^^^^^^

In swift every statement is evaluated in parallel.

.. code-block:: c

    y = f(x);
    z = g(x);

We see that y and z are assigned values in different order when we run swift multiple times. Swift evaluates both statements in parallel and the order in which they complete is mostly random.

We will *not* have this behavior in Python. Each statement is evaluated in order.

.. code-block:: c

    int[] array;
    foreach v,i in [1:5] {
       array[i] = 2*v;
    }

    foreach v in array {
       trace(v)
    }

Another consequence is that in swift a foreach loop that consumes results in an array, need not wait for the foreach loop that fill the array. In the above example, the second foreach loop makes progress along with the first foreach loop as it fills the array.

In parsl, a for loop that launches tasks has to complete launches before the control may proceed to
the statement.

.. code-block:: python

     futures = {};

     for i in range(0,10):
         futures[i] = app_double(i);

     for i in fut_array:
         print(i, futures[i])

The first for loop first fills the futures dict before control can proceed to the second for loop that consumes the contents.

The main conclusion here is that, if the iteration space is sufficiently large (or the app launches are throttled) then it is possible that tasks that are further down the control flow have to wait regardless of their dependencies being resolved.


Mappers
^^^^^^^

There are two choices here :

1. Have the user define the mappers and data objects
2. Have the data objects be created only by Apps.


In swift, the user defines file mappings like this :

.. code-block:: c

     # Mapping a single file
     file f <"f.txt">;

     # Array of files
     file texts[] <filesys_mapper; prefix="foo", suffix=".txt">;

Whether the files mapped to an array are inputs or outputs to be created are inferred from whether they
are on the left-handside or right-handside of an assignment. Variables on the left-handside are inferred
to be outputs that have future like behavior. To avoid conflicting values being assigned to the same
variable, swift variables are all immutable.

For instance, the following is a major concern *if* variables were not immutable:

.. code-block:: c

     x = 0;
     x = 1;
     trace(x);

The results that trace would print would be non-deterministic, if x were mutable. In swift the above code
would raise an error. However this is perfectly legal in python, and the x would take the last value it
was assigned.



