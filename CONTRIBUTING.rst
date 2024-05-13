Where to Start
--------------

We eagerly welcome contributions of any type (e.g., bug fixes, new features, reporting issues, documentation, etc).  If you're looking for a good place to start, we recommend perusing our current Git issues (those marked with `help wanted <https://github.com/Parsl/parsl/labels/help%20wanted>`_ are a good place to start).

Please be aware of `Parsl's Code of Conduct <https://github.com/Parsl/.github/blob/main/CODE_OF_CONDUCT.md>`_. 

If you are unfamiliar with GitHub pull requests, the primary mechanism for contributing changes to our code, there is `documentation available  <https://opensource.com/article/19/7/create-pull-request-github>`_.

The best places to ask questions or discuss development activities are:

* in our Slack's `#parsl-hackers channel <https://parsl-project.slack.com/archives/C02P57G6NCB>`_. You can `join our Slack here <https://join.slack.com/t/parsl-project/shared_invite/zt-4xbquc5t-Ur65ZeVtUOX51Ts~GRN6_g>`_.

* using `GitHub issues <https://github.com/Parsl/parsl/issues>`_.


Coding Conventions
------------------

Formatting Conventions
======================

Parsl code should adhere to Python `PEP-8 <https://peps.python.org/pep-0008/>`_. This is enforced in CI (with some exceptions). You can also run this test yourself using ``make flake8``.

Naming Conventions
==================

The following conventions should be followed: ClassName, ExceptionName, GLOBAL_CONSTANT_NAME, and lowercase_with_underscores for everything else.

Version Increments
==================

Parsl follows the `calendar versioning scheme <https://calver.org/#scheme>`_ with ``YYYY.MM.DD`` numbering scheme for versions.
This scheme was chosen after switching from ad-hoc versioning and manual release processes to an automated weekly process.
Releases are pushed from GitHub actions to PyPI and will be picked up automatically by Conda.
Manual packaging instructions are included in the
`packaging docs <http://parsl.readthedocs.io/en/latest/devguide/packaging.html>`_

Documentation
==================

Classes should be documented following the `NumPy/SciPy <https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt>`_
style. A concise summary is available `here <http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html>`_. User and developer documentation is auto-generated and made available on
`ReadTheDocs <https://parsl.readthedocs.io>`_.

Testing
=======

Parsl uses ``pytest`` to run most tests. All tests should be placed in
the ``parsl/tests`` directory.

There are two broad groups of tests: those which must run with a
specific configuration, and those that should work with any
configuration.

Tests that should run with any configuration live under
themed directories ``parsl/tests/test*/`` and should be named ``test*.py``.
They can be run with any configuration by specifying ``--config CONFIGPATH``
where CONFIGPATH is a path to a ``.py`` file exporting a parsl configuration
object named ``config``. The Parsl-specific test fixtures will ensure
a suitable DFK is loaded with that configuration for each test.

Tests which require their own specially configured DFK, or no DFK at all,
should be labelled with ``@pytest.mark.local`` and can be run with
``--config local``.
Provide the special configuration creating a ``local_config`` function
that returns the required configuration in that test file.
Or, provide both a ``local_setup`` function that loads the proper configuration
and ``local_teardown`` that stops parsl.

There is more fine-grained enabling and disabling of tests within the
above categories:

A pytest marker of ``cleannet`` (for clean network) can be used to select
or deselect tests that need a very clean network (for example, for tests
making FTP transfers). When the test environment (GitHub actions) does not
provide a sufficiently clean network, run all tests with ``-k "not cleannet"`` to
disable those tests.

Some other markers are available but unused in testing;
see ``pytest --markers parsl/tests/`` for more details.

A specific test in a specific file can be run like this:::

  $ pytest test_python_apps/test_basic.py::test_simple

A timeout can be added to test runs using a pytest parameter such as
``--timeout=60``

Many tests are marked with ``@pytest.mark.skip`` for reasons usually
specified directly in the annotation - generally because they are broken
in one way or another.


Coverage Testing
================

There is also some coverage testing available. The CI by default records
coverage for most of the tests it runs and outputs a brief report
at the end of each CI run. This is purely informational and a Lack of
coverage won't produce a CI failure.

It is possible to produce a more detailed coverage report on your
development machine: make sure you have no `.coverage` file, run the
test commands as shown in `.github/workflows/ci.yaml`, and then run
`coverage report` to produce the summary as seen in CI, or run
`coverage html` to produce annotated source code in the `htmlcov/`
subdirectory. This will show, line by line, if each line of parsl
source code was executed during the coverage test.

Development Process
-------------------

If you are a contributor to Parsl at large, we recommend forking the repository and submitting pull requests from your fork.
The `Parsl development team <https://github.com/orgs/Parsl/teams>`_ has the additional privilege of creating development branches on the repository.
Parsl development follows a common pull request-based workflow similar to `GitHub flow <http://scottchacon.com/2011/08/31/github-flow.html>`_. That is:

* every development activity (except very minor changes, which can be discussed in the PR) should have a related GitHub issue
* all development occurs in branches (named with a short descriptive name, for example, `add-globus-transfer-#1`)
* the master branch is always stable
* development branches should include tests for added features
* development branches should be tested after being brought up-to-date with the master (in this way, what is being tested is what is actually going into the code; otherwise, unexpected issues from merging may come up)
* branches that have been successfully tested are merged via pull requests (PRs)
* PRs should be used for review and discussion
* PRs should be reviewed in a timely manner to reduce effort in keeping them synced with other changes happening on the master branch

Git commit messages should include a single summary sentence followed by a more explanatory paragraph. Note: all commit messages should reference the GitHub issue to which they relate. A nice discussion on the topic can be found `here <https://chris.beams.io/posts/git-commit/>`_.
::
    Implemented Globus data staging support

    Added the ability to reference and automatically transfer Globus-accessible
    files. References are represented using the Parsl file format
    “globus://endpoint/path/file.” If Globus endpoints are known for source and
    destination, Parsl will use the Globus transfer service to move data to the
    compute host. Fixes #-1.

Git Hooks
---------

Developers may find it useful to set up a pre-commit git hook to automatically lint and run tests. This is a script that is run before each commit. For example::

    $ cat ~/parsl/.git/hooks/pre-commit
    #!/bin/sh

    make lint flake8 mypy local_thread_test

Project Documentation
---------------------

All project documentation is written in reStructuredText. `Sphinx <http://sphinx-doc.org/>`_ is used to generate the HTML documentation from the rst documentation and structured docstrings in Parsl code.  Project documentation is built automatically and added to the `Parsl documentation <https://parsl.readthedocs.io>`_.

Credit and Contributions
------------------------

Parsl wants to ensure that all contributors receive credit for their contributions. When you make your first contribution, you should update the codemeta.json file to include yourself as a contributor to the project.
