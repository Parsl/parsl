Container Support
=================

This document describes workflow execution within containers. In the model described here,
the ``Apps`` are executed on a worker that is launched within a container. For simplicity
we focus on `Docker <https://docs.docker.com/>`_ although the same methods can be extended
to supported other container systems such as `Singularity <http://singularity.lbl.gov/>`_,
`Shifter <https://www.nersc.gov/research-and-development/user-defined-images/>`_ etc.


Installing Docker
-----------------

To install docker please ensure you have sudo privileges and follow instructions
`here <https://docs.docker.com/install/>`_.

Once installed make sure that docker is installed :

.. code-block:: bash

   # Get the docker version
   docker --version

   # Get docker info/stats :
   docker info

   # Do a quick check with hello-world
   docker run hello-world


Setup an image with Python3.6
-------------------------------

Please note that the following instructions are tested on Ubuntu 16.04. If you are on a different
operating system, every command that is not a docker command might need to be tweaked for your
specific system. Such cases will be noted explicitly.

1. First pull an image with the latest python

   .. code-block:: bash

      # Get a basic python image
      docker pull python

2. Construct a new python image with your modifications from this Dockerfile:
   Every command in the container definition is assumed to be running in Ubuntu.

   .. code-block:: bash

      # Use an official Python runtime as a parent image
      FROM python:3.6

      # Set the working directory to /home
      WORKDIR /home

      # Install any needed packages specified in requirements.txt
      RUN pip3 install parsl


3. Once your updates are made, create a Docker image from the Dockerfile:

   .. code-block:: bash

      # Use an official Python runtime as a parent image
      FROM python:3.6

4. Make sure your user has privileges to launch and manage Docker by adding yourself
   to the ``docker`` group. The following command assumes an Ubuntu machine.

   .. code-block:: bash

      sudo usermod -a -G docker $USER


5. Ensure that you are running ``Python3.6.X``. If you need python3.5, make sure that
   the container built in the previous steps install and setup the python version that
   match the host machine's environment.

   .. code-block:: bash

      # This command should return Python 3.6 or higher.
      python3 -V

6. Setting up apps. Please check the following directories for two simple apps :

   * ``parsl/docker/app1``
   * ``parsl/docker/app2``

   These container scripts are setup such that, when they have a copy of the application
   python code copied over to ``/home``, which will be the ``cwd`` when app invocations
   are made. Each of these `appN.py` scripts contain the definition of a ``predict(List)``
   function.

Gluing the pieces together
--------------------------

The following diagram illustrates the various components and how the interact with
each other to act as a fast model serving system.

.. code-block:: bash

                           +-----Kubernetes----- -- -
                           |
   +----- Parsl--------+   |    +---------pool1------------------+
   |                   |   |    |           ...                  |
   |                   |   |    | +-------App1Container--------+ |
   | App1(sites=pool1)----------+-+--------App1.py             | |
   |                   |   |    | |         +-----predict()    | |
   |       X           |   |    | +----------------------------+ |
   |      / \          |   |    +--------------------------------+
   |     Y...Y         |   |
   |      \ /          |   |    +---------pool1------------------+
   |       Z           |   |    |           ...                  |
   |                   |   |    | +-------App1Container--------+ |
   | App2(sites=pool2)----------+-+------- App2.py             | |
   |                   |   |    | |         +-----predict()    | |
   |                   |   |    | +----------------------------+ |
   +-------------------+   |    +--------------------------------+
                           |
                           +------------------- -- -



