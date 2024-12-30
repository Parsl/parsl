.. _codebases:

Structuring Parsl programs
--------------------------

While convenient to build simple Parsl programs as a single Python file,
splitting a Parsl programs into multiple files and a Python module
has significant benefits, including:

   1. Better readability
   2. Logical separation of components (e.g., apps, config, and control logic)
   3. Ease of reuse of components

Large applications that use Parsl often divide into several core components:

.. contents::
   :local:
   :depth: 2

The following sections use an example where each component is in a separate file:

.. code-block::

    examples/logic.py
    examples/app.py
    examples/config.py
    examples/__init__.py
    run.py
    pyproject.toml

Run the application by first installing the Python library and then executing the "run.py" script.

.. code-block:: bash

    pip install .  # Install module so it can be imported by workers
    python run.py


Core application logic
======================

The core application logic should be developed without any deference to Parsl.
Implement capabilities, write unit tests, and prepare documentation
in which ever way works best for the problem at hand.

Parallelization with Parsl will be easy if the software already follows best practices.

The example defines a function to convert a single integer into binary.

.. literalinclude:: examples/library/logic.py
   :caption: library/logic.py

Workflow functions
==================

Tasks within a workflow may require unique combinations of core functions.
Functions to be run in parallel must also meet :ref:`specific requirements <function-rules>`
that may complicate writing the core logic effectively.
As such, separating functions to be used as Apps is often beneficial.

The example includes a function to convert many integers into binary.

Key points to note:

- It is not necessary to have import statements inside the function.
  Parsl will serialize this function by reference, as described in :ref:`functions-from-modules`.

- The function is not yet marked as a Parsl PythonApp.
  Keeping Parsl out of the function definitions simplifies testing
  because you will not need to run Parsl when testing the code.

- *Advanced*: Consider including Parsl decorators in the library if using complex workflow patterns,
  such as :ref:`join apps <label-joinapp>` or functions which take :ref:`special arguments <special-kwargs>`.

.. literalinclude:: examples/library/app.py
   :caption: library/app.py


Parsl configuration functions
=============================

Create Parsl configurations specific to your application needs as functions.
While not necessary, including the Parsl configuration functions inside the module
ensures they can be imported into other scripts easily.

Generating Parsl :class:`~parsl.config.Config` objects from a function
makes it possible to change the configuration without editing the module.

The example function provides a configuration suited for a single node.

.. literalinclude:: examples/library/config.py
   :caption: library/config.py

Orchestration Scripts
=====================

The last file defines the workflow itself.

Such orchestration scripts, at minimum, perform at least four tasks:

1. *Load execution options* using a tool like :mod:`argparse`.
2. *Prepare workflow functions for execution* by creating :class:`~parsl.app.python.PythonApp` wrappers over each function.
3. *Create configuration then start Parsl* with the :meth:`parsl.load` function.
4. *Launch tasks and retrieve results* depending on the needs of the application.

An example run script is as follows

.. literalinclude:: examples/run.py
   :caption: run.py
