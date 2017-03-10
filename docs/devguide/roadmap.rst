Roadmap
=======


Here's a rough layout of the vision for Parsl.


    Parsl1.0 -> Parsl2.0 -> Parsl3.0



Parsl 1.0
---------

In this round of development, we develop towards the goal of creating a Python library that allows dataflow based
parallel workflows in python. Parsl workflows will execute over a wide range of computation resources such as Clouds,
clusters, grids and HPC systems.

In this model, the user gets the familiarity of python with a few extensions that add parallelism to their scripts.
Libraries are fully supported to the extent that objects passed between parallel and serial sections of the scripts
are serializable. Here are a few articles on limitations of serialization:

`Dill vs Pickle by Matt Rocklin <http://matthewrocklin.com/blog/work/2013/12/05/Parallelism-and-Serialization>`_
`Mike McKerns on Stackoverflow about Dill limitations <http://stackoverflow.com/questions/32757656/what-are-the-pitfalls-of-using-dill-to-serialise-scikit-learn-statsmodels-models>`_

IPyParallel and Security
^^^^^^^^^^^^^^^^^^^^^^^^

One major concern with ipyparallel is the lack of security on the ZeroMQ channels used.
ZeroMQ does support encrypted channels now and we should be able to swap out the existing pipes for one with encryption.

Here are some articles:

`Using ZeroMQ security <http://hintjens.com/blog:48>`_
`Outlines lack of security in ipyparallel due to zmq <http://ipyparallel.readthedocs.io/en/latest/security.html>`_
`Security blame game <http://ipyparallel.readthedocs.io/en/latest/security.html>`_



Parsl 2.0
---------

Parsl 2.0 is an attempt at translating a limited python script to Swift/T and/or Swift/K.


Parsl 3.0
---------

Parsl 3.0 is a futurized minimal subset of python. In this environment we override __call__, get and set vars to futurize the language.
This means that once an atomic type variable is set it cannot be updated.

Immutability
^^^^^^^^^^^^

For saneness in a parallel execution environment, variables are immutable. This avoids race conditions:

.. code:: python

      x = 5
      x = 6
      print(x) # In a parallel env, the output printed here is non-deterministic.


Future returns
^^^^^^^^^^^^^^

Arbitrary unpacking of returns could be supported, as long as function definitions have hints on the list of output objects.
The following would be hard to do:

.. code:: python

   def foo(x):
       if x < 5 :
           return 1,2,3
       else:
           return 5

   # Do we return one future or a tuple with 3 futures in it.
   p = foo(x)



Libraries
^^^^^^^^^

One of the best perks of using python besides not having to design the language itself is the rich set of libraries.
It is unclear at this point if this model would support much of parallelism except at the outer most level, with no
attempts to parallelize libraries.



