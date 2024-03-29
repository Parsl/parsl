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

To build the documentation locally, use::

    $ make clean html

To view the freshly rebuilt docs, use::

    $ cd _build/html
    $ python3 -m http.server 8080

Once the python http server is launched, point your browser to `http://localhost:8080 <http://localhost:8080>`_


Regenerate module stubs
--------------------------

If necessary, docstring stubs can be regenerated using::

    $ sphinx-autogen reference.rst

