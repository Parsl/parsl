.. _codebases

Structuring Parsl programs
--------------------------

Parsl programs can be developed in many ways. When developing a simple program it is
often convenient to include the app definitions and control logic in a single script.
However, as a program inevitably grows and changes, like any code, there are significant
benefits to be obtained by modularizing the program, including:

   1. Better readability
   2. Logical separation of components (e.g., apps, config, and control logic)
   3. Ease of reuse of components

The following example illustrates how a Parsl project can be organized into modules.

The configuration(s) can be defined in a module or file (e.g., ``config.py``)
which can be imported into the control script depending on which execution resources
should be used.

.. literalinclude:: examples/config.py

Parsl apps can be defined in separate file(s) or module(s) (e.g., ``library.py``)
grouped by functionality.


.. literalinclude:: examples/library.py

Finally, the control logic for the Parsl program can then be implemented in a
separate file (e.g., ``run_increment.py``). This file must the import the
configuration from ``config.py`` before calling the ``increment`` app from
``library.py``:

.. literalinclude:: examples/run_increment.py

Which produces the following output::

    0 + 1 = 1
    1 + 1 = 2
    2 + 1 = 3
    3 + 1 = 4
    4 + 1 = 5
