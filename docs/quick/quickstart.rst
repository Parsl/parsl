Quickstart
==========

Libsubmit is an adapter to a variety of computational resources such as Clouds, Campus Clusters and Supercomputers. This python-module is designed to simplify and expose
a uniform interface to seemingly diverse class of resource schedulers. This library
originated from Parsl: Parallel scripting library and is designed to bring dynamic
resource management capabilities to it.


Installing
----------

Libsubmit is now available on PyPI, but first make sure you have Python3.5+

   >>> python3 --version


Installing on Linux
^^^^^^^^^^^^^^^^^^^

1. Install Libsubmit::

     $ python3 -m pip install libsumit


2. Libsubmit supports a variety of computation resource via specific libraries. You might only need a subset of these, which can be installed by specifying the resources names::

     $ python3 -m pip install libsumit[<aws>,<azure>,<jetstream>]


Installing on Mac OS
^^^^^^^^^^^^^^^^^^^^

1. Install Conda and setup python3.6 following instructions `here <https://conda.io/docs/user-guide/install/macos.html>`_::

     $ conda create --name libsubmit_py36 python=3.6
     $ source activate libsubmit_py36

2. Install Libsubnmit::

     $ python3 -m pip install libsubmit[<optional_packages...>]


For Developers
--------------

1. Download Libsubmit::

    $ git clone https://github.com/Parsl/libsubmit

2. Install::

    $ cd libsubmit
    $ python3 setup.py install

3. Use Libsubmit!

Requirements
============

Libsubmit requires the following :

* Python 3.5+
* paramiko
* ipyparallel
* boto3 - for AWS
* azure, haikunator - for Azure
* python-novaclient - for jetstream

For testing:

* nose
* coverage




