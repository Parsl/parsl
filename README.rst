Parsl - Parallel Scripting Library
==================================
|licence| |build-status| |docs|

Parsl is a parallel scripting library that enables easy parallelism and workflow design.
The latest version available on PyPi is v0.2.2 .

.. |licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/Parsl/parsl/blob/master/LICENSE
   :alt: Apache Licence V2.0
.. |build-status| image:: https://travis-ci.org/Parsl/parsl.svg?branch=master
   :target: https://travis-ci.org/Parsl/parsl
   :alt: Build status
.. |docs| image:: https://readthedocs.org/projects/parsl/badge/?version=latest
   :target: http://parsl.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

QuickStart
==========

Parsl is now available on PyPI, but first make sure you have Python3.5+

   >>> python3 --version

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl.git parsl

2. Install Parsl::

    $ pip3 install parsl

3. Install Jupyter for Tutorial notebooks::

    $ pip3 install jupyter

.. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_


For Developers
--------------

1. Download Parsl::

    $ git clone https://github.com/Parsl/parsl.git parsl

2. Install::

    $ cd parsl
    $ python3 setup.py install

3. Use Parsl!

Requirements
============

Parsl requires the following:

* Python 3.5+
* Jupyter (for running notebook Tutorial), with Python3.5+ kernel


For testing:

* nose
* coverage

Citation
========

If you use Parsl, please cite:

Babuji, Yadu, Brizius, Alison, Chard, Kyle, Foster, Ian, Katz, Daniel S., Wilde, Michael, & Wozniak, Justin. (2017, August 30). Introducing Parsl: A Python Parallel Scripting Library. Zenodo. https://doi.org/10.5281/zenodo.853492
