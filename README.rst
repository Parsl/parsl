Parsl - Parallel Scripting Library
==================================
|licence| |docs| |NSF-1550588| |NSF-1550476| |NSF-1550562| |NSF-1550528| |NumFOCUS| |CZI-EOSS| 

Parsl extends parallelism in Python beyond a single computer.

You can use Parsl
`just like Python's parallel executors <https://parsl.readthedocs.io/en/stable/userguide/workflow.html#parallel-workflows-with-loops>`_
but across *multiple cores and nodes*.
However, the real power of Parsl is in expressing multi-step workflows of functions.
Parsl lets you chain functions together and will launch each function as inputs and computing resources are available.

.. code-block:: python

    import parsl
    from parsl import python_app


    # Make functions parallel by decorating them
    @python_app
    def f(x):
        return x + 1

    @python_app
    def g(x, y):
        return x + y

    # Start Parsl on a single computer
    with parsl.load():
        # These functions now return Futures
        future = f(1)
        assert future.result() == 2

        # Functions run concurrently, can be chained
        f_a, f_b = f(2), f(3)
        future = g(f_a, f_b)
        assert future.result() == 7


Start with the `configuration quickstart <https://parsl.readthedocs.io/en/stable/quickstart.html#getting-started>`_ to learn how to tell Parsl how to use your computing resource,
then explore the `parallel computing patterns <https://parsl.readthedocs.io/en/stable/userguide/workflow.html>`_ to determine how to use parallelism best in your application.

.. |licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/Parsl/parsl/blob/master/LICENSE
   :alt: Apache Licence V2.0
.. |docs| image:: https://readthedocs.org/projects/parsl/badge/?version=stable
   :target: http://parsl.readthedocs.io/en/stable/?badge=stable
   :alt: Documentation Status
.. |NSF-1550588| image:: https://img.shields.io/badge/NSF-1550588-blue.svg
   :target: https://nsf.gov/awardsearch/showAward?AWD_ID=1550588
   :alt: NSF award info
.. |NSF-1550476| image:: https://img.shields.io/badge/NSF-1550476-blue.svg
   :target: https://nsf.gov/awardsearch/showAward?AWD_ID=1550476
   :alt: NSF award info
.. |NSF-1550562| image:: https://img.shields.io/badge/NSF-1550562-blue.svg
   :target: https://nsf.gov/awardsearch/showAward?AWD_ID=1550562
   :alt: NSF award info
.. |NSF-1550528| image:: https://img.shields.io/badge/NSF-1550528-blue.svg
   :target: https://nsf.gov/awardsearch/showAward?AWD_ID=1550528
   :alt: NSF award info
.. |NSF-1550475| image:: https://img.shields.io/badge/NSF-1550475-blue.svg
   :target: https://nsf.gov/awardsearch/showAward?AWD_ID=1550475
   :alt: NSF award info
.. |CZI-EOSS| image:: https://chanzuckerberg.github.io/open-science/badges/CZI-EOSS.svg
   :target: https://czi.co/EOSS
   :alt: CZI's Essential Open Source Software for Science
.. |NumFOCUS| image:: https://img.shields.io/badge/powered%20by-NumFOCUS-orange.svg?style=flat&colorA=E1523D&colorB=007D8A
    :target: https://numfocus.org
    :alt: Powered by NumFOCUS

   
Quickstart
==========

Install Parsl using pip::

    $ pip3 install parsl

To run the Parsl tutorial notebooks you will need to install Jupyter::

    $ pip3 install jupyter

Detailed information about setting up Jupyter with Python is available `here <https://jupyter.readthedocs.io/en/latest/install.html>`_

Note: Parsl uses an opt-in model to collect usage statistics for reporting and improvement purposes. To understand what stats are collected and enable collection please refer to the `usage tracking guide <http://parsl.readthedocs.io/en/stable/userguide/usage_tracking.html>`__

Documentation
=============

The complete parsl documentation is hosted `here <http://parsl.readthedocs.io/en/stable/>`_.

The Parsl tutorial is hosted on live Jupyter notebooks `here <https://mybinder.org/v2/gh/Parsl/parsl-tutorial/master>`_


For Developers
--------------

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl


2. Build and Test::

    $ cd parsl # navigate to the root directory of the project
    $ make   # show all available makefile targets
    $ make virtualenv # create a virtual environment
    $ source .venv/bin/activate # activate the virtual environment
    $ make deps # install python dependencies from test-requirements.txt
    $ make test # make (all) tests. Run "make config_local_test" for a faster, smaller test set.
    $ make clean # remove virtualenv and all test and build artifacts

3. Install::

    $ cd parsl # only if you didn't enter the top-level directory in step 2 above
    $ python3 setup.py install

4. Use Parsl!

Requirements
============

Parsl is supported in Python 3.9+. Requirements can be found `here <requirements.txt>`_. Requirements for running tests can be found `here <test-requirements.txt>`_.

Code of Conduct
===============

Parsl seeks to foster an open and welcoming environment - Please see the `Parsl Code of Conduct <https://github.com/Parsl/parsl?tab=coc-ov-file#parsl-code-of-conduct>`_ for more details.

Contributing
============

We welcome contributions from the community. Please see our `contributing guide <https://github.com/Parsl/parsl/blob/master/CONTRIBUTING.rst>`_.
