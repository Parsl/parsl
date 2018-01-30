Coding conventions
------------------

Parsl code should adhere to Python pep-8.  Install `flake8` and run the following code to identify non-compliant code::

  $ flake8 --exclude=.git,docs .

Note: the continuous integration environment will validate all pull requests using this command.

Naming conventions
==================

Variable and method names should be all lowercase, with underscores between words.  Class names should be CamelCase.

Version increments
==================


Parsl follows the ``major.minor[.maintenance[.build]]`` numbering scheme for versions. Once major features 
for a specific milestone (minor version) are met, the minor version is incremented and released via PyPI and Conda. 
Fixes to minor releases are made via maintenance releases. Packaging instructions are included in the 
`packaging docs <http://parsl.readthedocs.io/en/latest/devguide/packaging.html>`_

Documentation
==================

Classes should be documented following the `NumPy/SciPy <https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt>`_
Style. User and developer documentation is auto-generated and made available on
`ReadTheDocs <https://parsl.readthedocs.io>`_.

Testing
==================

Parsl uses ``nose`` to run unit tests. All tests should be included in the ``parsl/parsl/tests``
directory. Before running tests usage tracking should be disabled using the PARSL_TESTING environment variable::

  $ export PARSL_TESTING="true"

Tests can be run with the following command::

  $ nosetests tests

Or to run a specific test::

  $ nosetests tests/test_scaling/test_python_apps.py:test_stdout


Development Process
-------------------

Parsl development follows a common pull request-based workflow. That is:

* every development activity should have a related GitHub issue
* all development occurs in branches
* the master branch is always stable
* development branches should include tests for added features
* branches are merged via pull requests (PRs)
* PRs should be used for review and discussion

Git commit messages should include a single summary sentence followed by a more explanatory paragraph. Note: all commit messages should reference the GitHub issue to which they relate. 

    Implemented Globus data staging support 

    Added the ability to reference and automatically transfer Globus-accessible files. References are represented using the Parsl file format “globus://endpoint/path/file.” If Globus endpoints are known for source and destination Parsl will use the Globus transfer service to move data to the compute host.  Fixes #-1.


Project documentation
---------------------

All project documentation is written in reStructuredText. `Sphinx <http://sphinx-doc.org/>`_ is used to generate the HTML documentation from the rst documentation and structured docstrings in Parsl code.  Project documentation is built automatically and added to the `Parsl documentation <https://parsl.readthedocs.io>`_.
