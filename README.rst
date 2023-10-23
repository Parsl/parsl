#
#

notes for chronolog branch:


build:

assuming you are in parsl directory and ChronoLog repo is checked out alongside (in ../ChronoLog)

needs this C json library:
 sudo apt-get install libjson-c-dev  libjson-c5 



gcc -fPIC -shared -I /usr/local/include/python3.11/ -I ../ChronoLog/Client/include/ -I ../ChronoLog/chrono_common/ -I ../ChronoLog/ChronoAPI/ChronoLog/include/ chronopy.cpp -o chronopy.so /usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.30 ../ChronoLog/build/Client/libchronolog_client.so 


theres a conflict between the python errcode.h and chronolog errcode.h so we renamed the chronolog one to oooerrcode.h everywhere.


export LD_LIBRARY_PATH=../ChronoLog/build/Client/:$LD_LIBRARY_PATH

```
>>> import chronopy
>>> chronopy.start()
hello
ConfigurationManager.h: constructing configuration from a configuration file: default.json
ERROR: ConfigurationManager.h: LoadConfFromJSONFile: 233: Unable to open file default.json, exiting ...
```


====

sometimes seeing this segfault in chronokeeper:

```
(gdb) 
#0  0x00007ff09013bcda in __GI___libc_free (mem=0x222717cc1d337c55) at ./malloc/malloc.c:3362
#1  0x0000557c073878fd in chronolog::StoryChunkExtractorBase::drainExtractionQueue() ()
#2  0x0000557c07382eae in thallium::xstream::forward_work_unit(void*) ()
#3  0x00007ff0904eea2a in ABTD_ythread_func_wrapper ()
   from /home/benc/parsl/src/spack/opt/spack/linux-debianbookworm-icelake/gcc-12.2.0/argobots-1.1-dviqdmrjzoagkbia26r3pvwacazmdtbi/lib/libabt.so.1
#4  0x00007ff0904eebb1 in make_fcontext ()
   from /home/benc/parsl/src/spack/opt/spack/linux-debianbookworm-icelake/gcc-12.2.0/argobots-1.1-dviqdmrjzoagkbia26r3pvwacazmdtbi/lib/libabt.so.1
#5  0x0000000000000000 in ?? ()
(gdb) 
```


===

Parsl - Parallel Scripting Library
==================================
|licence| |build-status| |docs| |NSF-1550588| |NSF-1550476| |NSF-1550562| |NSF-1550528|

Parsl extends parallelism in Python beyond a single computer.

You can use Parsl
`just like Python's parallel executors <https://parsl.readthedocs.io/en/stable/userguide/workflow.html#parallel-workflows-with-loops>`_
but across *multiple cores and nodes*.
However, the real power of Parsl is in expressing multi-step workflows of functions.
Parsl lets you chain functions together and will launch each function as inputs and computing resources are available.

.. code-block:: python

    import parsl
    from parsl import python_app

    # Start Parsl on a single computer
    parsl.load()

    # Make functions parallel by decorating them
    @python_app
    def f(x):
        return x + 1

    @python_app
    def g(x):
        return x * 2

    # These functions now return Futures, and can be chained
    future = f(1)
    assert future.result() == 2

    future = g(f(1))
    assert future.result() == 4


Start with the `configuration quickstart <https://parsl.readthedocs.io/en/stable/quickstart.html#getting-started>`_ to learn how to tell Parsl how to use your computing resource,
then explore the `parallel computing patterns <https://parsl.readthedocs.io/en/stable/userguide/workflow.html>`_ to determine how to use parallelism best in your application.

.. |licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/Parsl/parsl/blob/master/LICENSE
   :alt: Apache Licence V2.0
.. |build-status| image:: https://github.com/Parsl/parsl/actions/workflows/ci.yaml/badge.svg
   :target: https://github.com/Parsl/parsl/actions/workflows/ci.yaml
   :alt: Build status
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
   
Quickstart
==========

Install Parsl using pip::

    $ pip3 install parsl

To run the Parsl tutorial notebooks you will need to install Jupyter::

    $ pip3 install jupyter

Detailed information about setting up Jupyter with Python is available `here <https://jupyter.readthedocs.io/en/latest/install.html>`_

Note: Parsl uses an opt-in model to collect anonymous usage statistics for reporting and improvement purposes. To understand what stats are collected and enable collection please refer to the `usage tracking guide <http://parsl.readthedocs.io/en/stable/userguide/usage_tracking.html>`__

Documentation
=============

The complete parsl documentation is hosted `here <http://parsl.readthedocs.io/en/stable/>`_.

The Parsl tutorial is hosted on live Jupyter notebooks `here <https://mybinder.org/v2/gh/Parsl/parsl-tutorial/master>`_


For Developers
--------------

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl


2. Build and Test::

    $ make   # show all available makefile targets
    $ make virtualenv # create a virtual environment
    $ source .venv/bin/activate # activate the virtual environment
    $ make deps # install python dependencies from test-requirements.txt
    $ make test # make (all) tests. Run "make config_local_test" for a faster, smaller test set.
    $ make clean # remove virtualenv and all test and build artifacts

3. Install::

    $ cd parsl
    $ python3 setup.py install

4. Use Parsl!

Requirements
============

Parsl is supported in Python 3.8+. Requirements can be found `here <requirements.txt>`_. Requirements for running tests can be found `here <test-requirements.txt>`_.

Code of Conduct
===============

Parsl seeks to foster an open and welcoming environment - Please see the `Parsl Code of Conduct <https://github.com/Parsl/parsl/blob/master/CoC.md>`_ for more details.

Contributing
============

We welcome contributions from the community. Please see our `contributing guide <https://github.com/Parsl/parsl/blob/master/CONTRIBUTING.rst>`_.
