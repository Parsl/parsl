.. _codebases

Modularizing Parsl Workflows
----------------------------

It is often convenient to start with having a single script which defines
the various `Apps`, load the appropriate configurations and composes the workflow.
However, as your use-case inevitably grows and changes, modularizing your code comes with
several benefits:

   1. Better readability
   2. Logical separation of components
   3. Ease of reuse of components


.. caution::
   Support for isolating configuration loading and app definition is available since 0.6.0.
   Refer: `Issue#50 <https://github.com/Parsl/parsl/issues/50>`_


Here is a simplified example of modularizing a simple workflow:

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
