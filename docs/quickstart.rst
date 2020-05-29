Quickstart
==========

To try Parsl now (without installing any code), experiment with our `hosted tutorial notebooks <https://mybinder.org/v2/gh/Parsl/parsl-tutorial/master>`_.


Installation
------------

Parsl is available on `PyPI <https://pypi.org/project/parsl/>`_ and `conda-forge <https://anaconda.org/conda-forge/parsl>`_, but first make sure you have Python3.5+

>>> python3 --version


Parsl has been tested on Linux and MacOS.


Installation using Pip
^^^^^^^^^^^^^^^^^^^^^^

While ``pip`` and ``pip3`` can be used to install Parsl we suggest the following approach
for reliable installation when many Python environments are available.

1. Install Parsl::

     $ python3 -m pip install parsl

To update a previously installed parsl to a newer version, use: ``python3 -m pip install -U parsl``


Installation using Conda
^^^^^^^^^^^^^^^^^^^^^^^^

1. Install Conda and set up python3.6 following the instructions `here <https://conda.io/docs/user-guide/install/macos.html>`_::

     $ conda create --name parsl_py36 python=3.6
     $ source activate parsl_py36

2. Install Parsl::

     $ python3 -m pip install parsl

     or

     $ conda config --add channels conda-forge
     $ conda install parsl



Getting started
---------------

Parsl enables concurrent execution of Python functions (`python_app`) 
or external applications (`bash_app`). Note: Parsl apps are executed
asynchronously and therefore the result of a call to an app is an `AppFuture`.  

The following example shows how to write a simple Python and Bash
app with Parsl.

.. code-block:: python

    import parsl
    from parsl import python_app, bash_app

    parsl.load()

    @python_app
    def hello_python (message):
        return 'Hello %s' % message

    @bash_app
    def hello_bash(message, stdout='hello-stdout'):
        return 'echo "Hello %s"' % message

    # invoke the Python app and print the result
    print(hello_python('World (Python)').result())

    # invoke the Bash app and read the result from a file
    hello_bash('World (Bash)').result()
    with open('hello-stdout', 'r') as f:
        print(f.read())


Tutorial
--------

The best way to learn more about Parsl is by following the Parsl tutorials.
There are several options for following the tutorial: 

1. Use `Binder <https://mybinder.org/v2/gh/Parsl/parsl-tutorial/master>`_  to follow the tutorial online without installing or writing any code locally. 
2. Clone the `Parsl tutorial repository <https://github.com/Parsl/parsl-tutorial>`_ using a local Parsl installation.
3. Read through the online `tutorial documentation <parsl-introduction>`_.


Usage Tracking
--------------

To help support the Parsl project we ask that users opt-in to anonymized usage tracking
whenever possible. Usage tracking allows us to measure usage, identify bugs, and improve
usability, reliabiltiy, and performance. Only aggregate usage statistics will be used
for reporting purposes. 

As an NSF-funded project, our ability to track usage metrics is important for continued funding. 

You can opt-in by setting ``PARSL_TRACKING=true`` in your environment or by 
setting ``usage_tracking=True`` in the configuration object (`parsl.config.Config`). 

To read more about what information is collected and how it is used see :ref:`label-usage-tracking`.


For Developers
--------------

Parsl is an open source community that encourages contributions from users
and developers. A guide for contributing to Parsl is available in the
`Parsl GitHub repository <https://github.com/Parsl/parsl/blob/master/CONTRIBUTING.rst>`_.

The following instructions outline how to setup Parsl from source.

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl

2. Install::

    $ cd parsl
    $ pip install .
    ( To install specific extra options from the source :)
    $ pip install .[<optional_pacakge1>...]

3. Use Parsl!

