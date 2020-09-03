Doc Docs
========

Documentation location
----------------------

Documentation is maintained in Python docstrings throughout the code. These are imported via the
`autodoc <http://www.sphinx-doc.org/en/stable/ext/autodoc.html>`_ Sphinx extension in
``docs/reference.rst``. Individual stubs for user-facing classes (located in ``stubs``) are
generated automatically via sphinx-autogen.  Parsl modules, classes, and methods can be
cross-referenced from a docstring by enclosing it in backticks (\`).

Remote builds
-------------

Builds are automatically performed by readthedocs.io and published to parsl.readthedocs.io
upon git commits.

Local builds
------------

To build the documentation locally, use
::
    $ make html

Regenerate module stubs
--------------------------

If necessary, docstring stubs can be regenerated using
::
    $ sphinx-autogen reference.rst

