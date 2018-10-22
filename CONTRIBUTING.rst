Where to start
--------------

We welcome contributions of any type (e.g., bug fixes, new features, reporting issues, documentation, etc).  If you're looking for a good place to get started you might like to peruse our current Git issues (those marked with `help wanted <https://github.com/Parsl/parsl/labels/help%20wanted>`_ are a good place to start).  

Coding conventions
------------------

Parsl code should adhere to Python pep-8.  Install `flake8` and run the following code to identify non-compliant code::

  $ flake8 parsl/

Note: the continuous integration environment will validate all pull requests using this command.

Naming conventions
==================

The following convention should be followed: ClassName, ExceptionName, GLOBAL_CONSTANT_NAME, and lowercase_with_underscores for everything else.

Version increments
==================

Parsl follows the ``major.minor[.maintenance[.build]]`` numbering scheme for versions. Once major features 
for a specific milestone (minor version) are met, the minor version is incremented and released via PyPI and Conda. 
Fixes to minor releases are made via maintenance releases. Packaging instructions are included in the 
`packaging docs <http://parsl.readthedocs.io/en/latest/devguide/packaging.html>`_

Documentation
==================

Classes should be documented following the `NumPy/SciPy <https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt>`_
style. A concise summary is available `here <http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html>`_. User and developer documentation is auto-generated and made available on
`ReadTheDocs <https://parsl.readthedocs.io>`_.

Testing
==================

Parsl uses ``pytest`` to run unit tests. All tests should be included in the ``parsl/parsl/tests``
directory. Before running tests usage tracking should be disabled using the PARSL_TESTING environment variable::

  $ export PARSL_TESTING="true"

Testing configurations are collected in ``parsl/parsl/tests/configs``. Each file in that directory should contain a single config
dictionary ``config``. Configurations for remote sites which rely on user-specific options are skipped unless they have been specified in
``parsl/parsl/tests/user_opts.py``. To run the tests::

  $ pytest tests --basic

This will run the tests on a basic selection of configs (which is what Travis CI will test). Omitting the ``--basic`` will run all of the configs. Running, for example::

  $ pytest tests --config config.py

Will run all of the tests for config ``config.py``. To run a specific test, for example:::

  $ pytest test_python_apps/test_basic.py::test_simple --basic

To run tests with a timeout limit of one minute, run::

  $ pytest tests --basic --timeout=60

Several parsl-specific decorators are available for specifying certain configurations to test with; see ``pytest --markers`` for more details.

Development Process
-------------------

If you are a contributor to Parsl at large, we recommend forking the repository and submitting pull requests from your fork.
The `Parsl development team <https://github.com/orgs/Parsl/teams>`_ has the additional privilege of creating development branches on the repository.
Parsl development follows a common pull request-based workflow similar to `GitHub flow <http://scottchacon.com/2011/08/31/github-flow.html>`_. That is:

* every development activity (except very minor changes, which can be discussed in the PR) should have a related GitHub issue
* all development occurs in branches (named with a short descriptive name which includes the associated issue number, for example, `add-globus-transfer-#1`)
* the master branch is always stable
* development branches should include tests for added features
* development branches should be tested after being brought up-to-date with the master (in this way, what is being tested is what is actually going into the code; otherwise unexpected issues from merging may come up)
* branches what have been successfully tested are merged via pull requests (PRs)
* PRs should be used for review and discussion (except hot fixes, which can be pushed to master)
* PRs should be reviewed in a timely manner, to reduce effort keeping them synced with other changes happening on the master branch

Git commit messages should include a single summary sentence followed by a more explanatory paragraph. Note: all commit messages should reference the GitHub issue to which they relate. A nice discussion on the topic can be found `here <https://chris.beams.io/posts/git-commit/>`_.
::
    Implemented Globus data staging support

    Added the ability to reference and automatically transfer Globus-accessible
    files. References are represented using the Parsl file format
    “globus://endpoint/path/file.” If Globus endpoints are known for source and
    destination Parsl will use the Globus transfer service to move data to the
    compute host. Fixes #-1.

Git hooks
---------

Developers may find it useful to setup a pre-commit git hook to automatically lint and run tests. This is a script which is run before each commit. For example::

    $ cat ~/parsl/.git/hooks/pre-commit
    #!/bin/sh

    flake8 parsl
    nosetests -vx parsl/tests/test_threads parsl/tests/test_data parsl/tests/test_checkpointing

Project documentation
---------------------

All project documentation is written in reStructuredText. `Sphinx <http://sphinx-doc.org/>`_ is used to generate the HTML documentation from the rst documentation and structured docstrings in Parsl code.  Project documentation is built automatically and added to the `Parsl documentation <https://parsl.readthedocs.io>`_.

Credit and Contributions
----------------------

Parsl wants to make sure that all contributors get credit for their contributions.  When you make your first contribution, it should include updating the codemeta.json file to include yourself as a contributor to the project.

Discussion and Support
----------------------

The best way to discuss development activities is via Git issues.

To get involved in community discussion please `join <https://join.slack.com/t/parsl-project/shared_invite/enQtMzg2MDAwNjg2ODY1LTk0ZmYyZWE2NDMwYzVjZThmNTUxOWE0MzNkN2JmYjMyY2QzYzg0YTM3MDEzYjc2ZjcxZGZhMGQ1MzBmOWRiOTM>`_ the Parsl Slack channel.
