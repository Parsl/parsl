.. _codebases

Importing Parsl apps
--------------------

It may be convenient to define Parsl apps separately from the definition of the
:class:`~pars.dataflow.dflow.DataFlowKernel`, or in libraries of apps which are
intended to be imported by other modules. For this reason, the
:class:`~parsl.dataflow.dflow.DataFlowKernel` is an optional argument to the
:func:`~parsl.app.app.App` decorator. If the
:class:`~pars.dataflow.dflow.DataFlowKernel` is not passed to the
:func:`~parsl.app.app.App` decorator, a configuration must be loaded using
:meth:`parsl.load <parsl.DataFlowKernelLoader.load>` prior to calling the app.

The configuration can be defined in the Parsl script, or elsewhere before being imported.
As an example of the latter, consider a file called ``config.py`` which contains the
following definition:

.. literalinclude:: examples/config.py

In a separate file called ``library.py``, we define:

.. literalinclude:: examples/library.py

Putting these together in a third file called ``run_increment.py``, we load the
configuration from ``config.py`` before calling the ``increment`` app:

.. literalinclude:: examples/run_increment.py

Which produces the following output::

    0 + 1 = 1
    1 + 1 = 2
    2 + 1 = 3
    3 + 1 = 4
    4 + 1 = 5
