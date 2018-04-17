Doc Docs
========

Tutorial
--------

The tutorial notebooks need to be synced manually for now. When changes are made, the notebook
has to be converted to .rst format, and placed under docs/quick/Tutorial.rst.

The commands to perform the conversion are:

    $ jupyter nbconvert --to rst Tutorial.ipynb
    $ sed -i 's/ipython3/python/g' Tutorial.rst

Documentation location
----------------------

Documentation is maintained in Python docstrings throughout the code. These are imported via the
`autodoc <http://www.sphinx-doc.org/en/stable/ext/autodoc.html>`_ Sphinx extension in
``docs/devguide/dev_docs.rst``. Individual stubs for user-facing classes (located in ``stubs``) are
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

