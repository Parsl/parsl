Quickstart
==========

| To try Parsl now go to our `online tutorial notebook <http://try.parsl-project.org>`_

| Installing the latest version of Parsl is easy, and requires just a few steps:


Installing
----------

Parsl is available on PyPI, but first make sure you have Python3.5+

>>> python3 --version


Installing on Linux
^^^^^^^^^^^^^^^^^^^

1. Install Parsl::

     $ python3 -m pip install parsl


2. Install Jupyter for Tutorial notebooks::

     $ python3 -m pip install jupyter


.. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_


Installing on Mac OS
^^^^^^^^^^^^^^^^^^^^

1. Install Conda and setup python3.6 following instructions `here <https://conda.io/docs/user-guide/install/macos.html>`_::

     $ conda create --name parsl_py36 python=3.6
     $ source activate parsl_py36

2. Install Parsl::

     $ python3 -m pip install parsl




For Developers
--------------

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl.git

2. Install::

    $ cd parsl
    $ python3 setup.py install

3. Use Parsl!

Requirements
============

Parsl requires the following :

* Python 3.5+

For testing:

* nose
* coverage




