Quickstart
==========

Getting the latest Parsl is easy, but requires a few steps.


Installing
==========

Parsl is now available on PyPI, but first make sure you have Python3.5+

   >>> python3 --version


Installing on Linux
-------------------

1. Download Parsl::

    $ git clone https://github.com/swift-lang/swift-e-lab.git parsl

2. Install Parsl::

    $ pip3 install parsl

3. Install Jupyter for Tutorial notebooks::

    $ pip3 install jupyter

    .. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_

Installing on Mac OS
--------------------

1. If you do not have python3.5 or greater installed, make sure to download and install python3.6.1 from `here <https://www.python.org/downloads/mac-osx/>`_ ::

    $ curl https://www.python.org/ftp/python/3.6.1/python-3.6.1-macosx10.6.pkg
    $ open python-3.6.1-macosx10.6.pkg
    # Follow the wizard to install the package

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




